package cluster

import (
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/segmentio/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZoneAffinityBalancer_UserData(t *testing.T) {

	ci, _ := os.LookupEnv("CI")
	if ci != "" {
		// todo : this isn't really portable...
		// NOTE: us-west-2 is simply where the circleci EC2 hosts reside. If this test fails then it's
		// likely that the EC2-detection logic in zone_affinity.go isn't working for a new EC2 setup.
		t.Run("us-west-2", func(t *testing.T) {
			b := ZoneAffinityBalancer{}
			zone := string(b.UserData())
			assert.True(t, strings.HasPrefix(zone, "us-west-2"), "expected a us-west-2 az but got %s", zone)
		})
	} else {
		t.Run("unknown zone", func(t *testing.T) {
			b := ZoneAffinityBalancer{}
			assert.Equal(t,"unknown", string(b.UserData()), "user data should be 'unknown'")
		})
	}

	t.Run("override zone", func(t *testing.T) {
		b := ZoneAffinityBalancer{Zone: "zone1"}
		assert.Equal(t, "zone1", string(b.UserData()))
	})
}

func TestZoneAffinityBalancer_Balance(t *testing.T) {

	b := ZoneAffinityBalancer{}

	brokers := map[string]*sarama.Broker{
		"z1": brokerWithRack("z1"),
		"z2": brokerWithRack("z2"),
		"z3": brokerWithRack("z3"),
		"":   {},
	}

	tests := []struct {
		name            string
		memberCounts    map[string]int
		partitionCounts map[string]int
		result          map[string]map[string]int
	}{
		{
			name: "unknown and known zones",
			memberCounts: map[string]int{
				"":   1,
				"z1": 1,
				"z2": 1,
			},
			partitionCounts: map[string]int{
				"z1": 5,
				"z2": 4,
				"":   9,
			},
			result: map[string]map[string]int{
				"z1": {"": 1, "z1": 5},
				"z2": {"": 2, "z2": 4},
				"":   {"": 6},
			},
		},
		{
			name: "all unknown",
			memberCounts: map[string]int{
				"": 5,
			},
			partitionCounts: map[string]int{
				"": 103,
			},
			result: map[string]map[string]int{
				"": {"": 103},
			},
		},
		{
			name: "remainder stays local",
			memberCounts: map[string]int{
				"z1": 3,
				"z2": 3,
				"z3": 3,
			},
			partitionCounts: map[string]int{
				"z1": 20,
				"z2": 19,
				"z3": 20,
			},
			result: map[string]map[string]int{
				"z1": {"z1": 20},
				"z2": {"z2": 19},
				"z3": {"z3": 20},
			},
		},
		{
			name: "imbalanced partitions",
			memberCounts: map[string]int{
				"z1": 1,
				"z2": 1,
				"z3": 1,
			},
			partitionCounts: map[string]int{
				"z1": 7,
				"z2": 0,
				"z3": 7,
			},
			result: map[string]map[string]int{
				"z1": {"z1": 5},
				"z2": {"z1": 2, "z3": 2},
				"z3": {"z3": 5},
			},
		},
		{
			name: "imbalanced members",
			memberCounts: map[string]int{
				"z1": 5,
				"z2": 3,
				"z3": 1,
			},
			partitionCounts: map[string]int{
				"z1": 9,
				"z2": 9,
				"z3": 9,
			},
			result: map[string]map[string]int{
				"z1": {"z1": 9, "z3": 6},
				"z2": {"z2": 9},
				"z3": {"z3": 3},
			},
		},
		{
			name: "no consumers in zone",
			memberCounts: map[string]int {
				"z2": 10,
			},
			partitionCounts: map[string]int {
				"z1": 20,
				"z3": 19,
			},
			result: map[string]map[string]int{
				"z2": {"z1": 20, "z3": 19},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// create members per the distribution in the test case.
			var members []Member
			for zone, count := range tt.memberCounts {
				for i := 0; i < count; i++ {
					members = append(members, Member{
						ID:       zone + ":" + strconv.Itoa(len(members)+1),
						UserData: []byte(zone),
					})
				}
			}

			// create partitions per the distribution in the test case.
			var partitions []Partition
			for zone, count := range tt.partitionCounts {
				for i := 0; i < count; i++ {
					partitions = append(partitions, Partition{
						ID:     int32(len(partitions)),
						Topic:  "test",
						Leader: brokers[zone],
					})
				}
			}

			res := b.Balance(members, partitions)

			// verification #1...all members must be assigned and with the
			// correct load.
			minLoad := len(partitions) / len(members)
			maxLoad := minLoad
			if len(partitions)%len(members) != 0 {
				maxLoad++
			}
			for _, member := range members {
				assignments, _ := res[member.ID]
				if len(assignments) < minLoad || len(assignments) > maxLoad {
					t.Errorf("expected between %d and %d partitions for member %s", minLoad, maxLoad, member.ID)
				}
			}

			// verification #2...all partitions are assigned, and the distribution
			// per source zone matches.
			partsPerZone := make(map[string]map[string]int)
			uniqueParts := make(map[int32]struct{})
			for id, assignments := range res {

				var member Member
				for _, m := range members {
					if id == m.ID {
						member = m
						break
					}
				}
				assert.NotEmpty(t, member.ID, "invalid member ID returned: %s", id)

				var partition Partition
				for _, id := range assignments {

					uniqueParts[id] = struct{}{}

					for _, p := range partitions {
						if p.ID == id {
							partition = p
							break
						}
					}
					if assert.NotEmpty(t, partition.Topic, "invalid partition ID returned: %d", id) {
						counts, ok := partsPerZone[string(member.UserData)]
						if !ok {
							counts = make(map[string]int)
							partsPerZone[string(member.UserData)] = counts
						}
						counts[partition.Leader.Rack()]++
					}
				}
			}

			require.Equal(t, len(partitions), len(uniqueParts), "not all partitions were assigned")
			require.Equal(t, tt.result, partsPerZone, "wrong balanced zones")
		})
	}
}

// brokerWithRack uses reflection to create a Broker with the rack field
// populated.
func brokerWithRack(rack string) *sarama.Broker {

	broker := &sarama.Broker{}
	pointerVal := reflect.ValueOf(broker)
	val := reflect.Indirect(pointerVal)

	member := val.FieldByName("rack")
	ptrToRack := unsafe.Pointer(member.UnsafeAddr())
	realPtrToRack := (**string)(ptrToRack)
	*realPtrToRack = &rack

	return broker
}
