package cntdb

import (
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Point", func() {
	var stdtime = unixTimestamp(1414141414)
	var subject Point

	BeforeEach(func() {
		var err error
		subject, err = NewPointAt("cpu", []string{"host:server-1", "dc:aws"}, stdtime.Time, 7)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should generate key/member names", func() {
		Expect(subject.keyName()).To(Equal("s:cpu,dc:aws,host:server-1:16367"))
		Expect(subject.memberName()).To(Equal("0543"))
	})

	It("should create", func() {
		Expect(subject.String()).To(Equal("cpu,dc:aws,host:server-1 1414141414 7\n"))

		pt, err := NewPointAt("cpu", nil, stdtime.Time, -2)
		Expect(err).NotTo(HaveOccurred())
		Expect(pt.String()).To(Equal("cpu 1414141414 -2\n"))

		_, err = NewPointAt(strings.Repeat("a", 51), nil, stdtime.Time, 1)
		Expect(err).To(Equal(errInvalidMetric))

		_, err = NewPointAt("cpu", []string{strings.Repeat("a", 51)}, stdtime.Time, 1)
		Expect(err).To(Equal(errInvalidTag))

		_, err = NewPointAt("cpu", []string{"bad tag"}, stdtime.Time, 1)
		Expect(err).To(Equal(errInvalidTag))
	})

	It("should parse", func() {
		tests := []struct {
			s string
			p Point
		}{
			{"cpu,dc:aws,host:server-1 1414141414 7\n",
				Point{"cpu", []string{"dc:aws", "host:server-1"}, stdtime, 7}},
			{"cpu 1414141414 -2",
				Point{"cpu", nil, stdtime, -2}},
			{"cpu,b,c,a 1414141414 1\n",
				Point{"cpu", []string{"a", "b", "c"}, stdtime, 1}},
		}

		for _, test := range tests {
			pt, err := ParsePoint(test.s)
			Expect(err).NotTo(HaveOccurred(), "for %s", test.s)
			Expect(pt).To(Equal(test.p), "for %s", test.s)
		}
	})

})
