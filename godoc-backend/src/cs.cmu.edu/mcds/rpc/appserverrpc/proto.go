package appserverrpc

import (
	"cs.cmu.edu/mcds/paxos"
)

type PauseArgs struct{}

type PauseReply struct {
	MaxSlot int
}

type UpdateStateArgs struct {
	Min       int
	Max       int
	Instances map[int]*paxos.Instance
	MaxSlot   int
}

type UpdateStateReply struct{}

type CopyStateArgs struct{}

type CopyStateReply struct {
	Min       int
	Max       int
	Instances map[int]*paxos.Instance
}

type ReplaceArgs struct {
	DeadAppServer    string
	DeadAppServerRpc string
	DeadPaxosServer  string
	DeadOTServer     string
	NewAppServer     string
	NewAppServerRpc  string
	NewPaxosServer   string
	NewOTServer      string
}

type ReplaceReply struct{}

type RestartArgs struct{}

type RestartReply struct{}
