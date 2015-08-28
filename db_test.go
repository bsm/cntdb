package cntdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DB", func() {
	var subject *DB
	var point = func(s string) *Point {
		pt, err := ParsePoint(s)
		Expect(err).NotTo(HaveOccurred())
		return pt
	}

	BeforeEach(func() {
		subject = NewDB("localhost:6379", 9)
	})

	AfterEach(func() {
		subject.client.FlushDb()
	})

	It("should write points", func() {
		subject.WritePoints([]Point{
			*point("cpu,host:a,dc:x 1414141414 2"),
			*point("cpu,dc:x,host:a 1414141414 4"),
			*point("cpu,host:b,dc:x 1414141414 3"),
			*point("cpu,host:a,dc:x 1414141414 -1"),
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
		subject.WritePoints([]Point{
			*point("cpu,a,b 1414141414 1"),
			*point("cpu,a,c 1414141414 1"),
			*point("cpu,b,c 1414141414 1"),
			*point("cpu,a,c 1414141414 1"),
			*point("mem,a,c 1414141414 1"),
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
			min, max := unixTimestamp(test.from).UnixDay(), unixTimestamp(1515151515).UnixDay()
			keys, err := subject.scopeKeys(test.met, test.tags, min, max)
			Expect(err).NotTo(HaveOccurred(), "for %+v", test)
			Expect(keys.Slice()).To(Equal(test.res), "for %+v", test)
		}
	})

	It("should query results", func() {
		subject.WritePoints([]Point{
			*point("cpu,a,b 1414141414 1"),
			*point("cpu,a,c 1414141414 2"),
			*point("cpu,b,c 1414141414 4"),
			*point("cpu,a,c 1414141414 8"),
			*point("mem,a,c 1414141414 16"),
		})

		res, err := subject.Query(&Criteria{
			Metric: "cpu",
			From:   time.Unix(1414141400, 0),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(ResultSet{
			{time.Unix(1414141380, 0).UTC(), 15},
		}))

		res, err = subject.Query(&Criteria{
			Metric:   "cpu",
			From:     time.Unix(1414141400, 0),
			Tags:     []string{"a"},
			Interval: time.Hour,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(ResultSet{
			{time.Unix(1414141200, 0).UTC(), 11},
		}))
	})

	It("should compact", func() {
		subject.WritePoints([]Point{
			*point("cpu,a,b 1414141414 1"),
			*point("cpu,a,c 1818181818 2"),
			*point("cpu,b,c 1414141414 4"),
			*point("cpu,a,c 1818181818 8"),
			*point("mem,a,c 1414141414 16"),
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
