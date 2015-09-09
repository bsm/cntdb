package cntdb

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidMetric = errors.New("cntdb: invalid metric name")
	errInvalidTag    = errors.New("cntdb: invalid tag name")
	errTooManyTags   = errors.New("cntdb: too many tags")
	errBadFormat     = errors.New("cntdb: bad format")
	errInvalidKey    = errors.New("cntdb: invalid key type")
)

type Point struct {
	metric    string
	tags      []string
	timestamp timestamp
	count     int64
}

func ParsePoint(raw string) (Point, error) {
	parts := strings.SplitN(strings.TrimSpace(raw), " ", 3)
	if len(parts) != 3 {
		return Point{}, errBadFormat
	}

	var tags []string
	mt := strings.Split(parts[0], ",")
	if len(mt) == 0 {
		return Point{}, errInvalidMetric
	} else if len(mt) > 1 {
		tags = mt[1:]
	}

	sec, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Point{}, errBadFormat
	}

	count, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Point{}, errBadFormat
	}

	return NewPointAt(mt[0], tags, time.Unix(sec, 0), count)
}

func NewPoint(metric string, tags []string, count int64) (Point, error) {
	return NewPointAt(metric, tags, time.Now(), count)
}

func NewPointAt(metric string, tags []string, at time.Time, count int64) (Point, error) {
	if len(metric) < 1 || len(metric) > 50 {
		return Point{}, errInvalidMetric
	} else if len(tags) > 50 {
		return Point{}, errTooManyTags
	}

	for _, tag := range tags {
		if len(tag) < 1 || len(tag) > 50 {
			return Point{}, errInvalidTag
		}
		for _, c := range tag {
			if (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != ':' && c != '-' && c != '_' {
				return Point{}, errInvalidTag
			}
		}
	}

	sort.Strings(tags)
	return Point{metric, tags, timestamp{at}, count}, nil
}

func (p Point) Series() string {
	return strings.Join(append([]string{p.metric}, p.tags...), ",")
}

func (p Point) String() string {
	return fmt.Sprintf("%s %d %d\n", p.Series(), p.timestamp.Unix(), p.count)
}

func (p Point) keyName() string {
	return fmt.Sprintf("s:%s:%d", p.Series(), p.timestamp.UnixDay())
}

func (p Point) memberName() string {
	return fmt.Sprintf("%04d", p.timestamp.MinuteOfDay())
}
