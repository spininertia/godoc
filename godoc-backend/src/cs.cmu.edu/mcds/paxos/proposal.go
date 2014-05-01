package paxos

type proposal struct {
	round          int
	slot           int
	highestSeqSeen int
	value          interface{}
}

func NewProposal(slot int, value interface{}) *proposal {
	return &proposal{
		slot:  slot,
		round: 1,
		value: value,
	}
}
