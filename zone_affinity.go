package cluster

import (
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

var _ Balancer = &ZoneAffinityBalancer{}

// ZoneAffinityBalancer makes a best effort to pair up consumers with partitions
// where the leader is in the same zone.  Doing so can help from a performance
// perspective by minimizing round trip latency, and it can also help keep costs
// down when running in AWS by avoid cross-AZ data transfer.  The primary
// objective is to spread partitions evenly across consumers with a secondary
// focus on maximizing the number of partitions where the leader and the
// consumer are in the same zone.
//
// Requires minimum Kafka protocol version of 0.10.0.0 in order to be able to
// determine the brokers' rack.
type ZoneAffinityBalancer struct {
	// Zone is the zone for this consumer.  It will be communicated to the
	// leader via UserData.  It will be inferred if left unset.  If the zone
	// cannot be determined, it will be reported as "unknown".
	Zone string

	lock     sync.Mutex
}

func (*ZoneAffinityBalancer) Name() string {
	return "zone-affinity"
}

func (*ZoneAffinityBalancer) Balance(members []Member, partitions []Partition) map[string][]int32 {
	zonedPartitions := make(map[string][]int32)
	for _, part := range partitions {
		zone := part.Leader.Rack()
		zonedPartitions[zone] = append(zonedPartitions[zone], part.ID)
	}

	zonedConsumers := make(map[string][]string)
	for _, member := range members {
		zone := string(member.UserData)
		zonedConsumers[zone] = append(zonedConsumers[zone], member.ID)
	}

	targetPerMember := len(partitions) / len(members)
	remainder := len(partitions) % len(members)
	assignments := make(map[string][]int32)

	// assign as many as possible in zone.  this will assign up to partsPerMember
	// to each consumer.  it will also prefer to allocate remainder partitions
	// in zone if possible.
	for zone, parts := range zonedPartitions {

		// shuffle the partitions prior to assignment.  if the topic is
		// semantically partitioned, then there's a possibility for hot
		// partitions.  we don't want the same partitions to go to the same
		// member on each rebalance, especially in cases where the consumer
		// group uses autoscaling.  shuffling makes it possible to try different
		// assignments with the hopes that the interaction between auto-scaling
		// and rebalancing will eventually hit a steady state.
		rand.Shuffle(len(parts), func(i, j int) {
			parts[i], parts[j] = parts[j], parts[i]
		})

		consumers := zonedConsumers[zone]
		if len(consumers) == 0 {
			continue
		}

		// don't over-allocate.  cap partition assignments at the calculated
		// target.
		partsPerMember := len(parts) / len(consumers)
		if partsPerMember > targetPerMember {
			partsPerMember = targetPerMember
		}

		for _, consumer := range consumers {
			assignments[consumer] = append(assignments[consumer], parts[:partsPerMember]...)
			parts = parts[partsPerMember:]
		}

		// if we had enough partitions for each consumer in this zone to hit its
		// target, attempt to use any leftover partitions to satisfy the total
		// remainder by adding at most 1 partition per consumer.
		leftover := len(parts)
		if partsPerMember == targetPerMember {
			if leftover > remainder {
				leftover = remainder
			}
			if leftover > len(consumers) {
				leftover = len(consumers)
			}
			remainder -= leftover
		}

		// this loop covers the case where we're assigning extra partitions or
		// if there weren't enough to satisfy the targetPerMember and the zoned
		// partitions didn't divide evenly.
		for i := 0; i < leftover; i++ {
			assignments[consumers[i]] = append(assignments[consumers[i]], parts[i])
		}
		parts = parts[leftover:]

		if len(parts) == 0 {
			delete(zonedPartitions, zone)
		} else {
			zonedPartitions[zone] = parts
		}
	}

	// assign out remainders regardless of zone.
	var remaining []int32
	for _, partitions := range zonedPartitions {
		remaining = append(remaining, partitions...)
	}
	// one last shuffle to ensure that the remaining partitions are
	// pseudo-randomly assigned irrespective of AZ.
	rand.Shuffle(len(remaining), func(i, j int) {
		remaining[i], remaining[j] = remaining[j], remaining[i]
	})

	for _, member := range members {
		assigned := assignments[member.ID]
		delta := targetPerMember - len(assigned)
		// if it were possible to assign the remainder in zone, it's been taken
		// care of already.  now we will portion out any remainder to a member
		// that can take it.
		if delta >= 0 && remainder > 0 {
			delta++
			remainder--
		}
		if delta > 0 {
			assignments[member.ID] = append(assigned, remaining[:delta]...)
			remaining = remaining[delta:]
		}
	}

	return assignments
}

func (b *ZoneAffinityBalancer) UserData() []byte {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.Zone == "" {
		b.Zone = findZone()
		if b.Zone == "" {
			b.Zone = "unknown"
		}
	}
	return []byte(b.Zone)
}

func findZone() string {
	switch whereAmI() {
	case "aws":
		return awsAvailabilityZone()
	}
	return ""
}

func whereAmI() string {
	// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/identify_ec2_instances.html
	for _, path := range [...]string{
		"/sys/devices/virtual/dmi/id/product_uuid",
		"/sys/hypervisor/uuid",
	} {
		b, err := ioutil.ReadFile(path)
		if err != nil {
			continue
		}
		s := string(b)
		switch {
		case strings.HasPrefix(s, "EC2"), strings.HasPrefix(s, "ec2"):
			return "aws"
		}
	}
	return "somewhere"
}

func awsAvailabilityZone() string {
	client := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			DisableKeepAlives:  true,
		},
	}
	r, err := client.Get("http://169.254.169.254/latest/meta-data/placement/availability-zone")
	if err != nil {
		return ""
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return ""
	}
	return string(b)
}
