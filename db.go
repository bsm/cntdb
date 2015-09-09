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

// WritePoints adds points to the DB
func (b *DB) WritePoints(points []Point) error {
	pipe := b.client.Pipeline()
	defer pipe.Close()

	seen := make(map[string]struct{}, len(points))
	for _, pt := range points {
		key := pt.keyName()
		pipe.ZIncrBy(key, float64(pt.count), pt.memberName())
		seen[key] = struct{}{}

		pipe.SAdd("m:"+pt.metric, key)
		for _, tag := range pt.tags {
			pipe.SAdd("t:"+tag, key)
		}
	}

	for key, _ := range seen {
		pipe.Expire(key, storageTTL)
	}

	_, err := pipe.Exec()
	return err
}

func (b *DB) Query(c *Criteria) (ResultSet, error) {
	from, until := c.getFrom(), c.getUntil()
	interval := c.getInterval()

	keys, err := b.scopeKeys(c.Metric, c.Tags, from, until)
	if err != nil {
		return nil, err
	}

	acc := make(map[time.Time]int64, 100)
	for _, key := range keys.Slice() {
		if err := b.scanSeries(key, from.Time, until.Time, func(r Result) error {
			acc[r.Timestamp.Truncate(interval)] += r.Value
			return nil
		}); err != nil {
			return nil, err
		}
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

// scans a series and applies callback to each result
func (b *DB) scanSeries(key string, from, until time.Time, callback func(Result) error) (err error) {
	series, err := parseSeries(key)
	if err != nil {
		return err
	}

	base := series.StartTime()
	min, max := from.Truncate(time.Minute), until.Truncate(time.Minute)

	var cursor int64
	for {
		var pairs []string
		cursor, pairs, err = b.client.ZScan(key, cursor, "", 100).Result()
		if err != nil {
			return
		}

		for i := 0; i < len(pairs); i += 2 {
			offset, _ := strconv.ParseInt(pairs[i], 10, 64)
			timestamp := base.Add(time.Duration(offset) * time.Minute)
			if timestamp.Before(min) || timestamp.After(max) {
				continue
			}

			value, _ := strconv.ParseInt(pairs[i+1], 10, 64)
			if err = callback(Result{timestamp, value}); err != nil {
				return
			}
		}

		if cursor == 0 {
			break
		}
	}
	return
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
