package cntdb

import (
	"strconv"
	"strings"
	"time"
)

type Criteria struct {
	From     time.Time
	Until    time.Time
	Metric   string
	Tags     []string
	Interval time.Duration
}

func (c *Criteria) getFrom() timestamp {
	if c == nil || c.From.IsZero() {
		return timestamp{time.Now().Add(-time.Hour)}
	}
	return timestamp{c.From}
}

func (c *Criteria) getUntil() timestamp {
	if c == nil || c.Until.IsZero() {
		return timestamp{time.Now()}
	}
	return timestamp{c.Until}
}

func (c *Criteria) getInterval() time.Duration {
	if c == nil || c.Interval < time.Minute {
		return time.Minute
	}
	return c.Interval
}

type Result struct {
	Timestamp time.Time
	Value     int64
}

type ResultSet []Result

func (p ResultSet) Len() int           { return len(p) }
func (p ResultSet) Less(i, j int) bool { return p[i].Timestamp.Before(p[j].Timestamp) }
func (p ResultSet) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// --------------------------------------------------------------------

type timestamp struct{ time.Time }

func unixTimestamp(sec int64) timestamp {
	return timestamp{time.Unix(sec, 0)}
}

func (t timestamp) UnixDay() int64 {
	return t.Unix() / 86400
}

func (t timestamp) MinuteOfDay() int64 {
	return t.Unix() % 86400 / 60
}

func parseUnixDay(key string) int64 {
	if pos := strings.LastIndex(key, ":"); pos+1 < len(key) {
		num, _ := strconv.ParseInt(key[pos+1:], 10, 64)
		return num
	}
	return 0
}
