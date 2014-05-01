package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"cs.cmu.edu/mcds/rpc/paxosrpc"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

type paxos struct {
	mu           *sync.Mutex
	mus          map[int]*sync.Mutex
	l            net.Listener
	dead         bool
	deaf         bool
	unreliable   bool
	rpcCount     int
	minEmptySlot int

	peers     []string
	partition []int // peers that are in the same network partition, for test usage, idx into peers[]
	me        int   // index into peers[]

	maxIns  int
	deafSeq int

	// Your data here.
	instances map[int]*Instance // slot number -> paxos instance
}

const (
	MAX_CLUSTER_SIZE = 9
)

var (
	LOGE = log.New(os.Stderr, "ERROR ", log.Lmicroseconds|log.Lshortfile)
	// LOGV = log.New(os.Stdout, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
	LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
	LOGS = log.New(os.Stdout, "State ", log.Lmicroseconds|log.Lshortfile)
)

type rpcReply struct {
	reply   interface{}
	success bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//

func call(srv string, name string, args interface{}, reply interface{}) bool {

	c, err := rpc.Dial("tcp", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// LOGE.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// LOGE.Println(err)
	return false
}

func asyncCall(srv string, name string, args interface{}, reply interface{}, replyChan chan *rpcReply, me int) {
	LOGV.Printf("%d Call:[%s %s]\n", me, srv, name)
	success := call(srv, name, args, reply)
	replyChan <- &rpcReply{success: success, reply: reply}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *paxos) Start(slot int, v interface{}) {
	// Your code here.
	proposal := NewProposal(slot, v)
	px.lock(slot)
	px.checkSlot(slot)
	px.unlock(slot)

	go px.propose(proposal)
}

// Proposer proposes value for a slot
func (px *paxos) propose(prop *proposal) {
	LOGV.Printf("Propose:[%s %d]\n", px.peers[px.me], prop.slot)

	for {
		px.lock(prop.slot)
		if px.instances[prop.slot].Decided {
			LOGV.Println("decided")
			px.unlock(prop.slot)
			break
		}
		px.unlock(prop.slot)
		var max_n_a = -1
		var max_v_a interface{}
		var proposedValue = prop.value
		LOGV.Println("Proposing value ", proposedValue)
		seq := px.genProposalSeq(prop)
		prop.highestSeqSeen = seq

		// send preare to all peers
		prepReplyChan := make(chan *rpcReply, len(px.peers))
		prepArgs := &paxosrpc.PrepareArgs{Slot: prop.slot, N: seq}

		var numPrepOk = 0
		var numRej = 0

		iterator, numPeer := px.peerIterator()
		for peer, ok := iterator(); ok; peer, ok = iterator() {
			if peer == px.peers[px.me] {
				selfPrepReply := new(paxosrpc.PrepareReply)
				px.Prepare(prepArgs, selfPrepReply)
				prepReplyChan <- &rpcReply{reply: selfPrepReply, success: true}
			} else {
				go asyncCall(peer, "Paxos.Prepare", prepArgs, new(paxosrpc.PrepareReply), prepReplyChan, px.me)
			}
		}

		for reply := range prepReplyChan {
			if reply.success {
				prepReply := reply.reply.(*paxosrpc.PrepareReply)
				if prepReply.Ok {
					if prepReply.N_a > max_n_a {
						max_n_a = prepReply.N_a
						max_v_a = prepReply.V_a
					}
					numPrepOk += 1
				} else {
					numRej += 1
				}
			} else {
				numRej += 1
			}

			if numPrepOk >= px.getMajority() || numRej >= px.getMajority() ||
				numRej+numPrepOk == len(px.partition) || numRej+numPrepOk == numPeer {
				break
			}
		}

		// accept phase
		if numPrepOk >= px.getMajority() {
			if max_n_a != -1 {
				LOGV.Println("Learned value from previous : ", max_n_a)
				proposedValue = max_v_a
			}

			accArgs := &paxosrpc.AcceptArgs{Slot: prop.slot, N: seq, Value: proposedValue}
			var numAccOk = 0
			var numRej = 0

			iterator, numPeer := px.peerIterator()
			accReplyChan := make(chan *rpcReply, len(px.peers))

			for peer, ok := iterator(); ok; peer, ok = iterator() {
				if peer == px.peers[px.me] {
					selfAccReply := new(paxosrpc.AcceptReply)
					px.Accept(accArgs, selfAccReply)
					accReplyChan <- &rpcReply{reply: selfAccReply, success: true}
				} else {
					go asyncCall(peer, "Paxos.Accept", accArgs, new(paxosrpc.AcceptReply), accReplyChan, px.me)
				}
			}

			for reply := range accReplyChan {
				if reply.success {
					accReply := reply.reply.(*paxosrpc.AcceptReply)

					if accReply.Ok {
						numAccOk += 1
					} else {
						numRej += 1
					}
				} else {
					numRej += 1
				}

				if numAccOk >= px.getMajority() || numRej >= px.getMajority() ||
					numRej+numAccOk == len(px.partition) || numRej+numAccOk == numPeer {
					break
				}
			}

			// decided
			if numAccOk >= px.getMajority() {
				cmtArgs := &paxosrpc.CommitArgs{Slot: prop.slot, Value: proposedValue}

				LOGV.Println("Commiting value: ", proposedValue, " at slot ", prop.slot)
				iterator, _ := px.peerIterator()

				for peer, ok := iterator(); ok; peer, ok = iterator() {
					if peer != px.peers[px.me] {
						go call(peer, "Paxos.Commit", cmtArgs, new(paxosrpc.CommitReply))
					} else {
						px.Commit(cmtArgs, new(paxosrpc.CommitReply))
					}
				}
				break
			}
		}

		time.Sleep(time.Duration(rand.Int()) % 20 * time.Millisecond)
	}
}

// Max returns the highest sequence number ever seen
func (px *paxos) Max() int {
	// Your code here.
	return px.maxIns
}

// Min returns the minimum empty slot number
func (px *paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	for {
		if instance, present := px.instances[px.minEmptySlot]; present {
			if instance.Decided {
				px.minEmptySlot += 1
			} else {
				break
			}
		} else {
			break
		}
	}

	return px.minEmptySlot
}

// Status quries stats for a specific slot
// return false, nil if undecided
// return true, nil if decided
func (px *paxos) Status(slot int) (bool, interface{}) {
	// Your code here.
	px.lock(slot)
	defer px.unlock(slot)

	if instance, present := px.instances[slot]; present {
		if instance.Decided {
			return true, instance.V_a
		}
	}

	return false, nil
}

// Kill this paxos instance
func (px *paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) Paxos {
	px := new(paxos)
	px.peers = peers
	px.me = me
	px.maxIns = -1
	px.deafSeq = 0
	px.instances = make(map[int]*Instance)
	px.mus = make(map[int]*sync.Mutex)
	px.partition = nil
	px.mu = new(sync.Mutex)
	px.minEmptySlot = 0

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(paxosrpc.Wrap(px))
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(paxosrpc.Wrap(px))

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		// os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("tcp", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go px.listenFromPeers(rpcs)
	}

	return px
}

// generate unique proposal sequence number
// round : num of round proposal
func (px *paxos) genProposalSeq(prop *proposal) int {
	prop.round = (prop.highestSeqSeen/MAX_CLUSTER_SIZE + 1)
	return MAX_CLUSTER_SIZE*prop.round + px.me
}

/***********************
 * RPC
 ***********************/

func (px *paxos) Prepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	// TODO: lock
	LOGV.Printf("%d Serve Prepare[%d %d]", px.me, args.Slot, args.N)
	px.lock(args.Slot)
	defer px.unlock(args.Slot)

	// newly added node act like deaf for proposals under deafSeq
	if args.Slot < px.deafSeq {
		LOGV.Printf("%d act like deaf for Prepare\n", px.me)
		reply.Ok = false
		return nil
	}

	px.updateMax(args.Slot)

	px.checkSlot(args.Slot)
	instance := px.instances[args.Slot]

	reply.Slot = args.Slot

	if args.N > instance.N_p {
		instance.N_p = args.N
		reply.Ok = true

		if instance.hasAccepted() {
			reply.N_a = instance.N_a
			reply.V_a = instance.V_a
		} else {
			reply.N_a = -1
			reply.V_a = -1
		}
	} else {
		reply.Ok = false
	}

	LOGV.Printf("Prepare Reply:[%s %s %d]", px.peers[px.me], reply.Ok, reply.N_a)

	return nil
}

func (px *paxos) Accept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	// TODO: lock
	px.lock(args.Slot)
	defer px.unlock(args.Slot)

	// newly added node act like deaf for proposals under deafSeq
	if args.Slot < px.deafSeq {
		LOGV.Printf("%d act like deaf for Accept\n", px.me)
		reply.Ok = false
		return nil
	}

	px.updateMax(args.Slot)

	LOGV.Printf("%d serve accept [%d %d]", px.me, args.Slot, args.N)
	px.checkSlot(args.Slot)
	instance := px.instances[args.Slot]

	reply.Slot = args.Slot

	if args.N >= instance.N_p {
		instance.N_p = args.N
		instance.N_a = args.N
		instance.V_a = args.Value
		reply.Ok = true
	} else {
		reply.Ok = false
	}

	LOGV.Printf("Accept Reply:[%s %s]", px.peers[px.me], reply.Ok)

	return nil
}

func (px *paxos) Commit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	// TODO: lock
	LOGV.Printf("Recv Commit [%d %d]\n", px.me, args.Slot, args.Value)
	px.updateMax(args.Slot)

	px.lock(args.Slot)
	defer px.unlock(args.Slot)

	px.checkSlot(args.Slot)
	instance := px.instances[args.Slot]

	if !instance.Decided {
		instance.Decided = true
		instance.V_a = args.Value
	}
	return nil
}

func (px *paxos) checkSlot(slot int) {
	if _, present := px.instances[slot]; !present {
		px.instances[slot] = NewInstance()
	}
}

//TODO: update for one mutex per slot
func (px *paxos) lock(slot int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, present := px.mus[slot]; !present {
		px.mus[slot] = new(sync.Mutex)
	}

	px.mus[slot].Lock()
}

func (px *paxos) unlock(slot int) {
	px.mus[slot].Unlock()
}

// start server and listen on port
// TODO: change to rdrop rate
func (px *paxos) listenFromPeers(rpcs *rpc.Server) {
	for px.dead == false {
		conn, err := px.l.Accept()
		if err == nil && px.dead == false {
			if px.deaf {
				// ignore request
				conn.Close()
			} else if px.unreliable && (rand.Int63()%1000) < 100 {
				// discard the request.
				conn.Close()
			} else if px.unreliable && (rand.Int63()%1000) < 200 {
				// process the request but force discard of reply.
				c1 := conn.(*net.TCPConn)
				f, _ := c1.File()
				err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
				if err != nil {
					fmt.Printf("shutdown: %v\n", err)
				}
				px.rpcCount++
				go rpcs.ServeConn(conn)
			} else {
				px.rpcCount++
				go rpcs.ServeConn(conn)
			}
		} else if err == nil {
			conn.Close()
		}
		if err != nil && px.dead == false {
			fmt.Printf("Paxos(%v) accept: %v\n", px.me, err.Error())
		}
	}
}

// iterate through the peers in the same network partition
// TODO: modify it to support network partition after passing the original mit tests
func (px *paxos) peerIterator() (func() (string, bool), int) {
	var idx = 0
	var numPeer int

	if px.partition == nil {
		numPeer = len(px.peers)
	} else {
		numPeer = len(px.partition)
	}

	return func() (string, bool) {
		if px.partition != nil {
			if idx >= len(px.partition) {
				return "", false
			} else {
				idx += 1
				return px.peers[px.partition[idx-1]], true
			}
		} else {
			if idx >= numPeer || idx >= len(px.peers) {
				return "", false
			} else {
				idx += 1
				return px.peers[idx-1], true
			}
		}
	}, numPeer
}

func (px *paxos) getMajority() int {
	return len(px.peers)/2 + 1
}

func (px *paxos) updateMax(slot int) {
	if slot > px.maxIns {
		px.maxIns = slot
	}
}

func (px *paxos) SetUnreliable(unreliable bool) {
	px.mu.Lock()
	px.unreliable = unreliable
	px.mu.Unlock()
}

func (px *paxos) GetUnreliable() bool {
	return px.unreliable
}

func (px *paxos) GetRPCCount() int {
	return px.rpcCount
}

func (px *paxos) SetPartition(partition []int) {
	px.mu.Lock()
	px.partition = partition
	px.mu.Unlock()
}

func (px *paxos) GetPartition() []int {
	return px.partition
}

func (px *paxos) SetDeaf(deaf bool) {
	px.mu.Lock()
	px.deaf = deaf
	px.mu.Unlock()
}

func (px *paxos) GetState() (int, int, map[int]*Instance) {
	return px.minEmptySlot, px.maxIns, px.instances
}

func (px *paxos) SetState(min, max, maxSlot int, instances map[int]*Instance) {
	px.minEmptySlot = min
	px.maxIns = max
	px.deafSeq = maxSlot

	for k, v := range instances {
		px.instances[k] = v
	}
}

func (px *paxos) Logging() {
	LOGS.Println("===============Current Paxos State================")
	LOGS.Println("Min", px.Min())
	LOGS.Println("Max", px.Max())
	LOGS.Println("Logs")

	for slot, ins := range px.instances {
		LOGS.Println(slot, ins.Decided, ins.V_a)
	}

	LOGS.Println("==================End Logging=====================")
}
