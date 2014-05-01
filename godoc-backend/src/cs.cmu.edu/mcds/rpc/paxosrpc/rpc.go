package paxosrpc

type RemotePaxos interface {
	Prepare(args *PrepareArgs, reply *PrepareReply) error
	Accept(args *AcceptArgs, reply *AcceptReply) error
	Commit(args *CommitArgs, reply *CommitReply) error
}

type Paxos struct {
	// embed all methods into the struct
	RemotePaxos
}

// Wrap wraps p in a type-safe wrapper struct to ensure that only
// Paxos methods are exported to receive RPCS

func Wrap(p RemotePaxos) RemotePaxos {
	return &Paxos{p}
}
