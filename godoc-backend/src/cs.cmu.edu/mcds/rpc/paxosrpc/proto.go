package paxosrpc

type PrepareArgs struct {
	Slot int
	N    int
}

type PrepareReply struct {
	Slot int
	Ok   bool
	N_a  int
	V_a  interface{}
}

type AcceptArgs struct {
	Slot  int
	N     int
	Value interface{}
}

type AcceptReply struct {
	Slot int
	Ok   bool
}

type CommitArgs struct {
	Slot  int
	Value interface{}
}

type CommitReply struct {
}
