package cntdb

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("series", func() {

	It("should parse series", func() {
		tests := []struct {
			key string
			ser series
		}{
			{"s:cpu,b,c:16367", series{"cpu", []string{"b", "c"}, 16367}},
			{"s:cpu:16367", series{"cpu", []string{}, 16367}},
			{"s:x:2", series{"x", []string{}, 2}},
		}

		for _, test := range tests {
			ser, err := parseSeries(test.key)
			Expect(err).NotTo(HaveOccurred(), "for %s", test.key)
			Expect(ser).To(Equal(test.ser), "for %s", test.key)
		}
	})

	It("should fail to parse bad keys", func() {
		tests := []string{
			"x:cpu,b,c:16367",
			"s:cpu,b,c:",
			"s:cpu:b,c",
			"s:cpu,b,c: 2134",
			"s::2",
			"s:x:",
			"",
		}

		for _, test := range tests {
			_, err := parseSeries(test)
			Expect(err).To(Equal(errInvalidKey), "for %s", test)
		}
	})

})

// --------------------------------------------------------------------

func point(s string) Point {
	pt, err := ParsePoint(s)
	if err != nil {
		Fail(err.Error())
	}
	return pt
}

func xmltime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t.Local()
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cntdb")
}
