package paxos

type Instance struct {
	N_p     int
	N_a     int
	V_a     interface{}
	Decided bool
}

func NewInstance() *Instance {
	return &Instance{
		N_p:     -1,
		N_a:     -1,
		V_a:     nil,
		Decided: false,
	}
}

func (ins *Instance) hasAccepted() bool {
	return ins.N_a != -1
}
