package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/segmentio/sarama"
)

var _ = Describe("Notification", func() {

	It("should init and convert", func() {
		n := newNotification(map[string][]int32{
			"a": {1, 2, 3},
			"b": {4, 5},
			"c": {1, 2},
		})
		Expect(n).To(Equal(&Notification{
			Type:    RebalanceStart,
			Current: map[string][]int32{"a": {1, 2, 3}, "b": {4, 5}, "c": {1, 2}},
		}))

		o := n.success(map[string][]int32{
			"a": {3, 4},
			"b": {1, 2, 3, 4},
			"d": {3, 4},
		})
		Expect(o).To(Equal(&Notification{
			Type:     RebalanceOK,
			Claimed:  map[string][]int32{"a": {4}, "b": {1, 2, 3}, "d": {3, 4}},
			Released: map[string][]int32{"a": {1, 2}, "b": {5}, "c": {1, 2}},
			Current:  map[string][]int32{"a": {3, 4}, "b": {1, 2, 3, 4}, "d": {3, 4}},
		}))
	})

})

var _ = Describe("balancer", func() {
	var topics map[string]topicInfo

	BeforeEach(func() {
		client := &mockClient{
			topics: map[string][]int32{
				"one":   {0, 1, 2, 3},
				"two":   {0, 1, 2},
				"three": {0, 1},
			},
		}

		var err error
		topics, err = topicInfoFromMetadata(client, map[string]sarama.ConsumerGroupMemberMetadata{
			"b": {Topics: []string{"three", "one"}},
			"a": {Topics: []string{"one", "two"}},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should parse from meta data", func() {
		Expect(topics).To(HaveLen(3))
	})

	It("should perform", func() {
		Expect(assign(Range, topics)).To(Equal(map[string]map[string][]int32{
			"a": {"one": {0, 1}, "two": {0, 1, 2}},
			"b": {"one": {2, 3}, "three": {0, 1}},
		}))

		Expect(assign(RoundRobin, topics)).To(Equal(map[string]map[string][]int32{
			"a": {"one": {0, 2}, "two": {0, 1, 2}},
			"b": {"one": {1, 3}, "three": {0, 1}},
		}))
	})

})

var _ = Describe("TopicInfo", func() {

	makeTopicInfo := func(memberIDs []string, nums []int32) topicInfo {
		members := make([]Member, len(memberIDs))
		for i, memberID := range memberIDs {
			members[i] = Member{ID: memberID}
		}
		partitions := make([]Partition, len(nums))
		for i, num := range nums {
			partitions[i] = Partition{Topic: "test", ID: num}
		}
		return topicInfo{members: members, partitions: partitions}
	}

	DescribeTable("Ranges",
		func(memberIDs []string, partitions []int32, expected map[string][]int32) {
			info := makeTopicInfo(memberIDs, partitions)
			Expect(Range.Balance(info.members, info.partitions)).To(Equal(expected))
		},

		Entry("three members, three partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("member ID order", []string{"M3", "M1", "M2"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("more members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1}, map[string][]int32{
			"M1": {0}, "M3": {1},
		}),
		Entry("far more members than partitions", []string{"M1", "M2", "M3"}, []int32{0}, map[string][]int32{
			"M2": {0},
		}),
		Entry("fewer members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2, 3}, map[string][]int32{
			"M1": {0}, "M2": {1, 2}, "M3": {3},
		}),
		Entry("uneven members/partitions ratio", []string{"M1", "M2", "M3"}, []int32{0, 2, 4, 6, 8}, map[string][]int32{
			"M1": {0, 2}, "M2": {4}, "M3": {6, 8},
		}),
	)

	DescribeTable("RoundRobin",
		func(memberIDs []string, partitions []int32, expected map[string][]int32) {
			info := makeTopicInfo(memberIDs, partitions)
			Expect(RoundRobin.Balance(info.members, info.partitions)).To(Equal(expected))
		},

		Entry("three members, three partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("member ID order", []string{"M3", "M1", "M2"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("more members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1}, map[string][]int32{
			"M1": {0}, "M2": {1},
		}),
		Entry("far more members than partitions", []string{"M1", "M2", "M3"}, []int32{0}, map[string][]int32{
			"M1": {0},
		}),
		Entry("fewer members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2, 3}, map[string][]int32{
			"M1": {0, 3}, "M2": {1}, "M3": {2},
		}),
		Entry("uneven members/partitions ratio", []string{"M1", "M2", "M3"}, []int32{0, 2, 4, 6, 8}, map[string][]int32{
			"M1": {0, 6}, "M2": {2, 8}, "M3": {4},
		}),
	)

})
