package cntdb

import (
	"sort"
	"strconv"
	"sync"
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

	keys, err := b.scopeKeys(c.Metric, c.Tags, from.UnixDay(), until.UnixDay())
	if err != nil {
		return nil, err
	}
	defer setPool.Put(keys)

	acc := make(map[time.Time]int64, 100)
	for _, key := range keys.Slice() {
		b.scanZSumInto(acc, key, from.Truncate(interval), until.Time, interval)
	}

	res := make(ResultSet, 0, len(acc))
	for ts, val := range acc {
		res = append(res, Result{ts, val})
	}
	sort.Sort(res)
	return res, err
}

// scope all keys that are relevant for the query
func (b *DB) scopeKeys(metric string, tags []string, min, max int64) (*strset.Set, error) {
	scope, err := b.scanIndex("m:"+metric, min, max)
	if err != nil {
		return nil, err
	}

	if len(tags) == 0 {
		return scope, nil
	}

	filters := makeSet()
	for _, tag := range tags {
		main := filters
		sub, err := b.scanIndex("t:"+tag, min, max)
		if err != nil {
			return nil, err
		}
		filters = main.Union(sub)
		setPool.Put(main)
		setPool.Put(sub)
	}

	result := filters.Intersect(scope)
	setPool.Put(filters)
	setPool.Put(scope)
	return result, nil
}

// scans a redis zset and sums values into acc
func (b *DB) scanZSumInto(acc map[time.Time]int64, key string, min, max time.Time, by time.Duration) (err error) {
	var cursor int64

	base := time.Unix(parseUnixDay(key)*86400, 0).UTC()
	for {
		var pairs []string
		cursor, pairs, err = b.client.ZScan(key, cursor, "", 100).Result()
		if err != nil {
			return
		}

		for i := 0; i < len(pairs); i += 2 {
			mins, _ := strconv.ParseInt(pairs[i], 10, 64)
			tstamp := base.Add(time.Duration(mins) * time.Minute).Truncate(by)
			if tstamp.Before(min) || tstamp.After(max) {
				continue
			}

			value, _ := strconv.ParseInt(pairs[i+1], 10, 64)
			acc[tstamp] += value
		}

		if cursor == 0 {
			break
		}
	}
	return
}

// scans a redis index via SSCAN to retrieve all keys
func (b *DB) scanIndex(key string, min, max int64) (matches *strset.Set, err error) {
	matches = makeSet()
	var cursor int64

	for {
		var members []string
		cursor, members, err = b.client.SScan(key, cursor, "", 100).Result()
		if err != nil {
			return
		}

		for _, member := range members {
			if num := parseUnixDay(member); num >= min && num <= max {
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
		if num := parseUnixDay(member); num < max {
			pipe.SRem(key, member)
		}
	}
	return nil
}

// --------------------------------------------------------------------

var setPool sync.Pool

func makeSet() *strset.Set {
	if v := setPool.Get(); v != nil {
		set := v.(*strset.Set)
		set.Clear()
		return set
	}
	return strset.New(100)
}
