package cntdb

import (
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsm/strset"
	"gopkg.in/redis.v3"
)

var storageTTL = 35 * 24 * time.Hour

type DB struct {
	client *redis.Client

	cursor int64 // compaction cursor
}

func NewDB(addr string, db int64) *DB {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &DB{client: client}
}

// Compact runs a compaction cycle
func (b *DB) Compact() error {
	max := timestamp{time.Now().Add(-storageTTL)}.UnixDay()
	pipe := b.client.Pipeline()
	defer pipe.Close()

	cursor, keys, err := b.client.Scan(atomic.LoadInt64(&b.cursor), "[mt]:*", 20).Result()
	if err != nil {
		return err
	}
	atomic.StoreInt64(&b.cursor, cursor)

	for _, key := range keys {
		if err := b.expire(key, pipe, max); err != nil {
			return err
		}
	}

	_, err = pipe.Exec()
	return err
}

// Set sets point values
func (b *DB) Set(points []Point) error {
	return b.writePoints(points, func(pipe *redis.Pipeline, key, member string, value int64) {
		pipe.ZAdd(key, redis.Z{Member: member, Score: float64(value)})
	})
}

// Increment increments point values to the DB
func (b *DB) Increment(points []Point) error {
	return b.writePoints(points, func(pipe *redis.Pipeline, key, member string, value int64) {
		pipe.ZIncrBy(key, float64(value), member)
	})
}

// QueryPoints performs a query and returns points
func (b *DB) QueryPoints(c *Criteria) ([]Point, error) {
	from, until := c.getFrom(), c.getUntil()
	interval := c.getInterval()
	keys, err := b.scopeKeys(c.Metric, c.Tags, from, until)
	if err != nil {
		return nil, err
	}

	index := make(map[string]Point, 100)
	err = b.scanSeries(keys.Slice(), from, until, func(s series, ts time.Time, val int64) error {
		point, err := NewPointAt(s.metric, s.tags, ts.Truncate(interval), val)
		if err != nil {
			return err
		}

		pointID := point.uID()
		if ex, ok := index[pointID]; ok {
			point.count += ex.count
		}
		index[pointID] = point
		return nil
	})

	points := make([]Point, 0, len(index))
	for _, point := range index {
		points = append(points, point)
	}
	return points, err
}

func (b *DB) Query(c *Criteria) (ResultSet, error) {
	from, until := c.getFrom(), c.getUntil()
	interval := c.getInterval()

	keys, err := b.scopeKeys(c.Metric, c.Tags, from, until)
	if err != nil {
		return nil, err
	}

	acc := make(map[time.Time]int64, 100)
	if err := b.scanSeries(keys.Slice(), from, until, func(_ series, ts time.Time, val int64) error {
		acc[ts.Truncate(interval)] += val
		return nil
	}); err != nil {
		return nil, err
	}

	res := make(ResultSet, 0, len(acc))
	for ts, val := range acc {
		res = append(res, Result{ts, val})
	}
	sort.Sort(res)
	return res, err
}

// scope all series keys that are relevant for the query
func (b *DB) scopeKeys(metric string, tags []string, from, until timestamp) (*strset.Set, error) {
	minDay, maxDay := from.UnixDay(), until.UnixDay()
	scope, err := b.scanIndex("m:"+metric, minDay, maxDay)
	if err != nil {
		return nil, err
	}

	if len(tags) == 0 {
		return scope, nil
	}

	filters := strset.New(10)
	for _, tag := range tags {
		sub, err := b.scanIndex("t:"+tag, minDay, maxDay)
		if err != nil {
			return nil, err
		}
		filters = filters.Union(sub)
	}
	return scope.Intersect(filters), nil
}

// scans multiple series and applies callback to each result
func (b *DB) scanSeries(keys []string, from, until timestamp, callback func(series, time.Time, int64) error) error {
	min, max := from.Truncate(time.Minute), until.Truncate(time.Minute)

	pipe := b.client.Pipeline()
	defer pipe.Close()

	// parse/validate keys, build pipeline
	series := make([]series, len(keys))
	cmds := make([]*redis.ZSliceCmd, len(keys))
	for n, key := range keys {
		ser, err := parseSeries(key)
		if err != nil {
			return err
		}
		series[n] = ser
		cmds[n] = pipe.ZRangeWithScores(key, 0, -1)
	}
	_, _ = pipe.Exec()

	// iterate over series, process results
	for n, ser := range series {
		base := ser.StartTime()
		pairs, err := cmds[n].Result()
		if err != nil {
			return err
		}

		for _, pair := range pairs {
			offset, _ := strconv.ParseInt(pair.Member.(string), 10, 64)
			timestamp := base.Add(time.Duration(offset) * time.Minute)
			if timestamp.Before(min) || timestamp.After(max) {
				continue
			}

			if err := callback(ser, timestamp, int64(pair.Score)); err != nil {
				return err
			}
		}
	}
	return nil
}

// scans a redis index via SSCAN to retrieve all keys
func (b *DB) scanIndex(key string, minDay, maxDay int64) (matches *strset.Set, err error) {
	matches = strset.New(10)
	var cursor int64

	for {
		var members []string
		cursor, members, err = b.client.SScan(key, cursor, "", 100).Result()
		if err != nil {
			return
		}

		for _, member := range members {
			var series series
			if series, err = parseSeries(member); err != nil {
				return
			} else if series.unixDay >= minDay && series.unixDay <= maxDay {
				matches.Add(member)
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}

// writes points
func (b *DB) writePoints(points []Point, forEach func(*redis.Pipeline, string, string, int64)) error {
	pipe := b.client.Pipeline()
	defer pipe.Close()

	seen := make(map[string]struct{}, len(points))
	for _, pt := range points {
		key := pt.keyName()
		forEach(pipe, key, pt.memberName(), pt.count)
		seen[key] = struct{}{}

		pipe.SAdd("m:"+pt.metric, key)
		for _, tag := range pt.tags {
			pipe.SAdd("t:"+tag, key)
		}
	}

	for key := range seen {
		pipe.Expire(key, storageTTL)
	}

	_, err := pipe.Exec()
	return err
}

// expire appends expiration commands to a redis pipeline
func (b *DB) expire(key string, pipe *redis.Pipeline, max int64) error {
	members, err := b.client.SRandMemberN(key, 100).Result()
	if err != nil {
		return err
	}

	for _, member := range members {
		ser, err := parseSeries(member)
		if err != nil {
			return err
		} else if ser.unixDay < max {
			pipe.SRem(key, member)
		}
	}
	return nil
}
