package cntdb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DB", func() {
	var subject *DB

	BeforeEach(func() {
		subject = NewDB("localhost:6379", 9)
	})

	AfterEach(func() {
		subject.client.FlushDb()
	})

	It("should set", func() {
		subject.Set([]Point{
			point("cpu,host:a,dc:x 1414141414 2"),
			point("cpu,dc:x,host:a 1414141414 4"),
			point("cpu,host:b,dc:x 1414141414 3"),
			point("cpu,host:a,dc:x 1414141414 1"),
		})

		Expect(subject.client.Keys("*").Val()).To(ConsistOf([]string{
			"s:cpu,dc:x,host:b:16367",
			"s:cpu,dc:x,host:a:16367",
			"m:cpu",
			"t:host:b",
			"t:host:a",
			"t:dc:x",
		}))
		Expect(subject.client.TTL("s:cpu,dc:x,host:a:16367").Val()).To(BeNumerically("~", storageTTL, time.Second))
		Expect(subject.client.TTL("s:cpu,dc:x,host:b:16367").Val()).To(BeNumerically("~", storageTTL, time.Second))

		Expect(subject.client.SMembers("m:cpu").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:a:16367", "s:cpu,dc:x,host:b:16367"}))
		Expect(subject.client.SMembers("t:host:a").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:a:16367"}))
		Expect(subject.client.SMembers("t:host:b").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:b:16367"}))
		Expect(subject.client.SMembers("t:dc:x").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:a:16367", "s:cpu,dc:x,host:b:16367"}))

		v1 := subject.client.ZRangeWithScores("s:cpu,dc:x,host:a:16367", 0, -1).Val()
		Expect(v1).To(HaveLen(1))
		Expect(v1[0].Score).To(Equal(1.0))
		Expect(v1[0].Member).To(Equal("0543"))
		v2 := subject.client.ZRangeWithScores("s:cpu,dc:x,host:b:16367", 0, -1).Val()
		Expect(v2).To(HaveLen(1))
		Expect(v2[0].Score).To(Equal(3.0))
		Expect(v2[0].Member).To(Equal("0543"))
	})

	It("should increment", func() {
		subject.Increment([]Point{
			point("cpu,host:a,dc:x 1414141414 2"),
			point("cpu,dc:x,host:a 1414141414 4"),
			point("cpu,host:b,dc:x 1414141414 3"),
			point("cpu,host:a,dc:x 1414141414 -1"),
		})

		Expect(subject.client.Keys("*").Val()).To(ConsistOf([]string{
			"s:cpu,dc:x,host:b:16367",
			"s:cpu,dc:x,host:a:16367",
			"m:cpu",
			"t:host:b",
			"t:host:a",
			"t:dc:x",
		}))
		Expect(subject.client.TTL("s:cpu,dc:x,host:a:16367").Val()).To(BeNumerically("~", storageTTL, time.Second))
		Expect(subject.client.TTL("s:cpu,dc:x,host:b:16367").Val()).To(BeNumerically("~", storageTTL, time.Second))

		Expect(subject.client.SMembers("m:cpu").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:a:16367", "s:cpu,dc:x,host:b:16367"}))
		Expect(subject.client.SMembers("t:host:a").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:a:16367"}))
		Expect(subject.client.SMembers("t:host:b").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:b:16367"}))
		Expect(subject.client.SMembers("t:dc:x").Val()).To(ConsistOf([]string{"s:cpu,dc:x,host:a:16367", "s:cpu,dc:x,host:b:16367"}))

		v1 := subject.client.ZRangeWithScores("s:cpu,dc:x,host:a:16367", 0, -1).Val()
		Expect(v1).To(HaveLen(1))
		Expect(v1[0].Score).To(Equal(5.0))
		Expect(v1[0].Member).To(Equal("0543"))
		v2 := subject.client.ZRangeWithScores("s:cpu,dc:x,host:b:16367", 0, -1).Val()
		Expect(v2).To(HaveLen(1))
		Expect(v2[0].Score).To(Equal(3.0))
		Expect(v2[0].Member).To(Equal("0543"))
	})

	It("should scope keys", func() {
		subject.Set([]Point{
			point("cpu,a,b 1414141414 1"),
			point("cpu,a,c 1414141414 1"),
			point("cpu,b,c 1414141414 1"),
			point("cpu,a,c 1414141414 1"),
			point("mem,a,c 1414141414 1"),
		})

		tests := []struct {
			met  string
			from int64
			tags []string
			res  []string
		}{
			{"oth", 1414141400, nil, []string{}},
			{"cpu", 1414141400, nil, []string{"s:cpu,a,b:16367", "s:cpu,a,c:16367", "s:cpu,b,c:16367"}},
			{"cpu", 1414141400, []string{"x"}, []string{}},
			{"cpu", 1414141400, []string{"a"}, []string{"s:cpu,a,b:16367", "s:cpu,a,c:16367"}},
			{"cpu", 1414141400, []string{"b"}, []string{"s:cpu,a,b:16367", "s:cpu,b,c:16367"}},
			{"cpu", 1414141400, []string{"c"}, []string{"s:cpu,a,c:16367", "s:cpu,b,c:16367"}},
			{"mem", 1414141400, []string{"a"}, []string{"s:mem,a,c:16367"}},
			{"mem", 1414141400, []string{"b"}, []string{}},
			{"mem", 1414141400, []string{"c"}, []string{"s:mem,a,c:16367"}},

			{"cpu", 1420000000, nil, []string{}},
			{"cpu", 1420000000, []string{"a"}, []string{}},
			{"mem", 1420000000, []string{"c"}, []string{}},
		}

		for _, test := range tests {
			from, until := unixTimestamp(test.from), unixTimestamp(1515151515)
			keys, err := subject.scopeKeys(test.met, test.tags, from, until)
			Expect(err).NotTo(HaveOccurred(), "for %+v", test)
			Expect(keys.Slice()).To(Equal(test.res), "for %+v", test)
		}
	})

	It("should query results", func() {
		subject.Set([]Point{
			point("cpu,a,b 1414141200 1"),  // 2014-10-24T09:00:00Z
			point("cpu,a,c 1414141300 2"),  // 2014-10-24T09:01:40Z
			point("cpu,a,c 1414142000 4"),  // 2014-10-24T09:13:20Z
			point("cpu,b,c 1414146000 8"),  // 2014-10-24T10:20:00Z
			point("cpu,a,b 1414200000 16"), // 2014-10-25T01:20:00Z
			point("cpu,b,c 1414230000 32"), // 2014-10-25T09:40:00Z
			point("mem,a,c 1414141200 64"),
		})

		tests := []struct {
			crit Criteria
			res  ResultSet
		}{
			// from 09:00 until open end, by hour
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T09:00:00Z"), Interval: time.Hour}, ResultSet{
				{xmltime("2014-10-24T09:00:00Z"), 7},
				{xmltime("2014-10-24T10:00:00Z"), 8},
				{xmltime("2014-10-25T01:00:00Z"), 16},
				{xmltime("2014-10-25T09:00:00Z"), 32},
			}},
			// from 10:00 until open end, by day
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T10:00:00Z"), Interval: 24 * time.Hour}, ResultSet{
				{xmltime("2014-10-24T00:00:00Z"), 8},
				{xmltime("2014-10-25T00:00:00Z"), 48},
			}},
			// between 09:00 and 09:10, by minute
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T09:00:00Z"), Until: xmltime("2014-10-24T09:10:00Z"), Interval: time.Minute}, ResultSet{
				{xmltime("2014-10-24T09:00:00Z"), 1},
				{xmltime("2014-10-24T09:01:00Z"), 2},
			}},
			// between 08:59 and 09:01, by minute
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T08:59:00Z"), Until: xmltime("2014-10-24T09:01:00Z"), Interval: time.Minute}, ResultSet{
				{xmltime("2014-10-24T09:00:00Z"), 1},
				{xmltime("2014-10-24T09:01:00Z"), 2},
			}},
			// between 09:01 and 09:03, by minute
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T09:01:00Z"), Until: xmltime("2014-10-24T09:03:00Z"), Interval: time.Minute}, ResultSet{
				{xmltime("2014-10-24T09:01:00Z"), 2},
			}},
			// between 09:00 and 09:12:59, by 10-mins
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T09:00:00Z"), Until: xmltime("2014-10-24T09:12:59Z"), Interval: 10 * time.Minute}, ResultSet{
				{xmltime("2014-10-24T09:00:00Z"), 3},
			}},
			// between 09:00 and 09:12:59, by 10-mins
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T09:00:00Z"), Until: xmltime("2014-10-24T09:13:00Z"), Interval: 10 * time.Minute}, ResultSet{
				{xmltime("2014-10-24T09:00:00Z"), 3},
				{xmltime("2014-10-24T09:10:00Z"), 4},
			}},

			// tagged with 'a' between 09:00 and 11:00, by hour
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T09:00:00Z"), Until: xmltime("2014-10-24T11:00:00Z"), Tags: []string{"a"}, Interval: time.Hour}, ResultSet{
				{xmltime("2014-10-24T09:00:00Z"), 7},
			}},

			// `from` is after the last point
			{Criteria{Metric: "cpu", From: xmltime("2014-10-25T09:41:00Z"), Interval: time.Minute}, ResultSet{}},
			// `until` is before the first point
			{Criteria{Metric: "cpu", From: xmltime("2014-10-24T08:00:00Z"), Until: xmltime("2014-10-24T08:59:00Z"), Interval: time.Minute}, ResultSet{}},
		}

		for _, test := range tests {
			res, err := subject.Query(&test.crit)
			Expect(err).NotTo(HaveOccurred(), "for %+v", test.crit)
			Expect(res).To(Equal(test.res), "for %+v", test.crit)
		}
	})

	It("should query points", func() {
		subject.Set([]Point{
			point("cpu,a,b 1414141200 1"), // 2014-10-24T09:00:00Z
			point("cpu,a,c 1414141300 2"), // 2014-10-24T09:01:40Z
			point("cpu,a,c 1414142000 4"), // 2014-10-24T09:13:20Z
			point("cpu,b,c 1414146000 8"), // 2014-10-24T10:20:00Z
		})

		points, err := subject.QueryPoints(&Criteria{
			Metric:   "cpu",
			From:     xmltime("2014-10-24T09:00:00Z"),
			Interval: time.Hour,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(points).To(ConsistOf([]Point{
			point("cpu,a,b 1414141200 1"), // 2014-10-24T09:00:00Z
			point("cpu,a,c 1414141200 6"), // 2014-10-24T09:00:00Z
			point("cpu,b,c 1414144800 8"), // 2014-10-24T10:00:00Z
		}))
	})

	It("should query-store", func() {
		subject.Set([]Point{
			point("cpu,a,b 1414141200 1"), // 2014-10-24T09:00:00Z
			point("cpu,a,c 1414141300 2"), // 2014-10-24T09:01:40Z
			point("cpu,a,c 1414142000 4"), // 2014-10-24T09:13:20Z
			point("cpu,b,c 1414146000 8"), // 2014-10-24T10:20:00Z

			point("cpu.1h,b,c 1414144800 7"), // 2014-10-24T10:00:00Z
		})

		err := subject.QueryStore("cpu.1h", &Criteria{
			Metric:   "cpu",
			From:     xmltime("2014-10-24T08:00:00Z"),
			Interval: time.Hour,
		})
		Expect(err).NotTo(HaveOccurred())

		points, err := subject.QueryPoints(&Criteria{
			Metric:   "cpu.1h",
			From:     xmltime("2014-10-24T08:00:00Z"),
			Interval: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(points).To(ConsistOf([]Point{
			point("cpu.1h,a,b 1414141200 1"), // 2014-10-24T09:00:00Z
			point("cpu.1h,a,c 1414141200 6"), // 2014-10-24T09:00:00Z
			point("cpu.1h,b,c 1414144800 8"), // 2014-10-24T10:00:00Z
		}))
	})

	It("should compact", func() {
		subject.Set([]Point{
			point("cpu,a,b 1414141414 1"),
			point("cpu,a,c 1818181818 2"),
			point("cpu,b,c 1414141414 4"),
			point("cpu,a,c 1818181818 8"),
			point("mem,a,c 1414141414 16"),
		})

		Expect(subject.client.Keys("*").Val()).To(ConsistOf([]string{
			"s:cpu,a,b:16367",
			"s:cpu,b,c:16367",
			"s:cpu,a,c:21043",
			"s:mem,a,c:16367",
			"m:cpu",
			"m:mem",
			"t:a",
			"t:b",
			"t:c",
		}))
		Expect(subject.Compact()).NotTo(HaveOccurred())
		Expect(subject.client.Keys("*").Val()).To(ConsistOf([]string{
			"s:cpu,a,b:16367",
			"s:cpu,b,c:16367",
			"s:cpu,a,c:21043",
			"s:mem,a,c:16367",
			"m:cpu",
			"t:a",
			"t:c",
		}))
	})

})

func benchWrites(b *testing.B, batchSize int, tagsMap map[string]int) {
	// Set batch size
	if testing.Short() {
		batchSize /= 10
	}

	// Prepare available tag choices
	choices := make([][]string, 0, len(tagsMap))
	for pattern, count := range tagsMap {
		tags := make([]string, count)
		for i := 0; i < count; i++ {
			tags[i] = fmt.Sprintf(pattern, i)
		}
		choices = append(choices, tags)
	}

	// Connect
	client := NewDB("127.0.0.1:6379", 9)
	defer client.client.FlushDb()

	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		batch := make([]Point, batchSize)
		for n := 0; n < batchSize; n++ {
			tags := make([]string, len(choices))
			for k, list := range choices {
				tags[k] = list[rand.Intn(len(list))]
			}

			point, _ := NewPoint("cpu", tags, 2)
			batch[n] = point
		}

		if err := client.Set(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSet_1kMetrics_HugeBatches(b *testing.B) {
	benchWrites(b, 10000, map[string]int{"host:srv-%d": 10, "port:%d": 10, "inst:m%d": 10})
}

func BenchmarkSet_1kMetrics_LargeBatches(b *testing.B) {
	benchWrites(b, 1000, map[string]int{"host:srv-%d": 10, "port:%d": 10, "inst:m%d": 10})
}

func BenchmarkSet_1kMetrics_SmallBatches(b *testing.B) {
	benchWrites(b, 100, map[string]int{"host:srv-%d": 10, "port:%d": 1, "inst:m%d": 10})
}

func BenchmarkSet_100Metrics_LargeBatches(b *testing.B) {
	benchWrites(b, 1000, map[string]int{"host:srv-%d": 10, "port:%d": 1, "inst:m%d": 10})
}

func BenchmarkQuery_Parallel(b *testing.B) {
	client := NewDB("127.0.0.1:6379", 9)
	defer client.client.FlushDb()

	err := client.Set([]Point{
		point("cpu,a,b 1414141414 1"),
		point("cpu,a,c 1414141414 2"),
		point("cpu,b,c 1414141414 4"),
		point("cpu,a,c 1414141414 8"),
		point("mem,a,c 1414141414 16"),
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Query(&Criteria{
				Metric: "cpu",
				From:   time.Unix(1414141400, 0),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
