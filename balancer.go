package cluster

import (
	"math"
	"sort"

	"github.com/segmentio/sarama"
)

// NotificationType defines the type of notification
type NotificationType uint8

// String describes the notification type
func (t NotificationType) String() string {
	switch t {
	case RebalanceStart:
		return "rebalance start"
	case RebalanceOK:
		return "rebalance OK"
	case RebalanceError:
		return "rebalance error"
	}
	return "unknown"
}

const (
	UnknownNotification NotificationType = iota
	RebalanceStart
	RebalanceOK
	RebalanceError
)

// Notification are state events emitted by the consumers on rebalance
type Notification struct {
	// Type exposes the notification type
	Type NotificationType

	// Claimed contains topic/partitions that were claimed by this rebalance cycle
	Claimed map[string][]int32

	// Released contains topic/partitions that were released as part of this rebalance cycle
	Released map[string][]int32

	// Current are topic/partitions that are currently claimed to the consumer
	Current map[string][]int32
}

func newNotification(current map[string][]int32) *Notification {
	return &Notification{
		Type:    RebalanceStart,
		Current: current,
	}
}

func (n *Notification) success(current map[string][]int32) *Notification {
	o := &Notification{
		Type:     RebalanceOK,
		Claimed:  make(map[string][]int32),
		Released: make(map[string][]int32),
		Current:  current,
	}
	for topic, partitions := range current {
		o.Claimed[topic] = int32Slice(partitions).Diff(int32Slice(n.Current[topic]))
	}
	for topic, partitions := range n.Current {
		o.Released[topic] = int32Slice(partitions).Diff(int32Slice(current[topic]))
	}
	return o
}

// --------------------------------------------------------------------

// Member describes an individual participant in the consumer group.  It will
// contain the UserData from the JoinGroupRequest (if provided).
type Member struct {
	ID       string
	UserData []byte
}

// Partition describes a single partition that is to be consumed.
type Partition struct {
	Topic  string
	ID     int32
	Leader *sarama.Broker
}

// Balancer is a strategy for dividing partitions to be consumed across group
// members.
type Balancer interface {
	// Name is the name of this balancer.  It is sent in the JoinGroupRequest
	// in the ConsumerGroupMemberMetadata.
	Name() string

	// Balancer returns a map of member id to assigned partitions.  This will
	// only be run on the Consumer Group Leader, and the results will be sent
	// back to the cluster via the SyncGroupResponse.
	Balance(members []Member, partitions []Partition) map[string][]int32

	// UserData is any additional information that should be provided in the
	// ConsumerGroupMetadata in the JoinGroupRequest.  If the balancer does not
	// require any custom data, then this function should return nil.
	UserData() []byte
}

var _ Balancer = &builtinBalancer{}

type builtinBalancer struct {
	name string
	fn   func(members []Member, partitions []Partition) map[string][]int32
}

func (bb *builtinBalancer) Name() string {
	return bb.name
}

func (bb *builtinBalancer) Balance(members []Member, partitions []Partition) map[string][]int32 {
	return bb.fn(members, partitions)
}

func (bb *builtinBalancer) UserData() []byte {
	return nil
}

// Range is the default and assigns partition ranges to consumers.
// Example with six partitions and two consumers:
//   C1: [0, 1, 2]
//   C2: [3, 4, 5]
var Range = &builtinBalancer{
	name: string(StrategyRange),
	fn: func(members []Member, partitions []Partition) map[string][]int32 {
		sort.Slice(members, func(i, j int) bool {
			return members[i].ID < members[j].ID
		})

		mlen := len(members)
		plen := len(partitions)
		res := make(map[string][]int32, mlen)

		for pos, member := range members {
			n, i := float64(plen)/float64(mlen), float64(pos)
			min := int(math.Floor(i*n + 0.5))
			max := int(math.Floor((i+1)*n + 0.5))
			sub := partitions[min:max]
			if len(sub) > 0 {
				parts := make([]int32, len(sub))
				for i := range sub {
					parts[i] = sub[i].ID
				}
				res[member.ID] = parts
			}
		}
		return res
	},
}

// RoundRobin assigns partitions by alternating over consumers.
// Example with six partitions and two consumers:
//   C1: [0, 2, 4]
//   C2: [1, 3, 5]
var RoundRobin = &builtinBalancer{
	name: string(StrategyRoundRobin),
	fn: func(members []Member, partitions []Partition) map[string][]int32 {
		sort.Slice(members, func(i, j int) bool {
			return members[i].ID < members[j].ID
		})

		mlen := len(members)
		res := make(map[string][]int32, mlen)
		for i, pnum := range partitions {
			memberID := members[i%mlen].ID
			res[memberID] = append(res[memberID], pnum.ID)
		}
		return res
	},
}

type topicInfo struct {
	members    []Member
	partitions []Partition
}

func topicInfoFromMetadata(client sarama.Client, members map[string]sarama.ConsumerGroupMemberMetadata) (map[string]topicInfo, error) {
	topics := make(map[string]topicInfo)
	for id, meta := range members {
		for _, name := range meta.Topics {
			topic, ok := topics[name]
			if !ok {
				nums, err := client.Partitions(name)
				if err != nil {
					return nil, err
				}
				for _, num := range nums {
					leader, err := client.Leader(name, num)
					if err != nil {
						return nil, err
					}
					topic.partitions = append(topic.partitions, Partition{
						Topic:  name,
						ID:     num,
						Leader: leader,
					})
				}
			}
			topic.members = append(topic.members, Member{
				ID:       id,
				UserData: meta.UserData,
			})
			topics[name] = topic
		}
	}
	return topics, nil
}

func assign(balancer Balancer, topics map[string]topicInfo) map[string]map[string][]int32 {
	res := make(map[string]map[string][]int32, 1)
	for name, t := range topics {
		for memberID, partitions := range balancer.Balance(t.members, t.partitions) {
			if _, ok := res[memberID]; !ok {
				res[memberID] = make(map[string][]int32, 1)
			}
			res[memberID][name] = partitions
		}
	}
	return res
}
