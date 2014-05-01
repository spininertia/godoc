// this file defines paxos API appserver to call

package paxos

type Paxos interface {
	// Starts begins a paxos instance in slot number slot
	// the method should not block but return immediately
	Start(slot int, v interface{})

	// Status checks the status for paxos instance in slot number slot
	// The method will return a bool value indicating whether the the slot is decided
	// if decided, it will also return the chosen value
	Status(slot int) (bool, interface{})

	// Max returns the maximum sequence number this Paxos peer has ever seen
	// The method can be helpful when for crash-recovering
	// The newly added paxos peer should asks every other paxos peer the max value
	// it then should not participate in these paxos instances
	Max() int

	// Min returns the minimum empty slot number
	// The method is useful for Caller to know which slot to propose next
	// When used together with Max, the Caller can know whether there is empty slot to fill in
	// If there are empty slots, the Caller should propose no-op value
	Min() int

	//

	// Kill kills a paxos instance
	// This method is only used for testing
	Kill()

	// GetState get state from a paxos instance
	GetState() (int, int, map[int]*Instance)

	// SetState sets state for a paxos isntance
	// This is mainly used for a newly added node
	SetState(min, max, maxSlot int, instances map[int]*Instance)

	Logging()

	// Set Unreliable
	SetUnreliable(bool)

	GetUnreliable() bool

	GetRPCCount() int

	SetPartition([]int)

	GetPartition() []int

	SetDeaf(bool)
}
