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
		return timestamp{time.Now().Add(-time.Hour).UTC()}
	}
	return timestamp{c.From.UTC()}
}

func (c *Criteria) getUntil() timestamp {
	if c == nil || c.Until.IsZero() {
		return timestamp{time.Now().UTC()}
	}
	return timestamp{c.Until.UTC()}
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

type series struct {
	metric  string
	tags    []string
	unixDay int64
}

func (s series) StartTime() time.Time {
	return time.Unix(s.unixDay*86400, 0).UTC()
}

func parseSeries(key string) (s series, err error) {
	if len(key) < 2 || key[:2] != "s:" {
		return s, errInvalidKey
	}
	key = key[2:]

	piv := strings.LastIndex(key, ":")
	if piv < 1 || piv+2 > len(key) {
		return s, errInvalidKey
	}
	if s.unixDay, err = strconv.ParseInt(key[piv+1:], 10, 64); err != nil {
		return s, errInvalidKey
	}

	parts := strings.Split(key[:piv], ",")
	if len(parts) < 1 || parts[0] == "" {
		return s, errInvalidKey
	}

	s.metric = parts[0]
	s.tags = parts[1:]
	return
}

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
