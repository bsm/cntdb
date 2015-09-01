package cntdb

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Helpers", func() {

	It("should parse epoch day", func() {
		tests := []struct {
			str string
			num int64
		}{
			{"s:cpu,b,c:16367", 16367},
			{"s:cpu,b,c:", 0},
			{"s:cpu,b,c: 2134", 0},
			{"", 0},
		}

		for _, test := range tests {
			Expect(parseUnixDay(test.str)).To(Equal(test.num), "for %+v", test)
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

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cntdb")
}
