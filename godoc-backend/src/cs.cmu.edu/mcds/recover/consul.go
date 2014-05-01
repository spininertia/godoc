package main

import (
	"cs.cmu.edu/mcds/rpc/appserverrpc"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"syscall"
)

var (
	LOGE = log.New(os.Stderr, "ERROR ", log.Lmicroseconds|log.Lshortfile)
	LOGV = log.New(os.Stdout, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
	// LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
)

type rpcReply struct {
	reply   interface{}
	success bool
}

type Consul struct {
	aliveAppServersRpc []string
	deadAppServer      string
	deadAppServerRpc   string
	deadPaxosServer    string
	deadOTServer       string
	newAppServer       string
	newAppServerRpc    string
	newPaxosServer     string
	newOTServer        string
}

func NewConsul(config string) *Consul {
	b, err := ioutil.ReadFile(config)
	if err != nil {
		panic(err)
	}
	s := string(b)
	lines := strings.Split(s, "\n")
	consul := new(Consul)
	consul.aliveAppServersRpc = strings.Split(lines[0], " ")
	consul.deadPaxosServer = lines[1]
	consul.deadAppServer = lines[2]
	consul.deadAppServerRpc = lines[3]
	consul.deadOTServer = lines[4]
	consul.newPaxosServer = lines[5]
	consul.newAppServer = lines[6]
	consul.newAppServerRpc = lines[7]
	consul.newOTServer = lines[8]

	LOGV.Println("Alive App Server list:", consul.aliveAppServersRpc)
	LOGV.Println("Dead App Server:", consul.deadAppServer)
	LOGV.Println("Dead App Server Rpc:", consul.deadAppServerRpc)
	LOGV.Println("Dead Paxos Server:", consul.deadPaxosServer)
	LOGV.Println("Dead OT Server", consul.deadOTServer)
	LOGV.Println("Added App Server:", consul.newPaxosServer)
	LOGV.Println("Added App Server Rpc:", consul.newAppServerRpc)
	LOGV.Println("Added Paxos Server:", consul.newPaxosServer)
	LOGV.Println("Added OT Server:", consul.newOTServer)

	return consul
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	LOGV.Printf("Call:[%s %s]\n", srv, name)
	c, err := rpc.DialHTTP("tcp", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			LOGE.Printf("consul Dial() %s failed: %v\n", srv, err1)
		}
		return false
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	LOGE.Println(err)
	return false
}

func asyncCall(srv string, name string, args interface{}, reply interface{}, replyChan chan *rpcReply) {

	success := call(srv, name, args, reply)
	replyChan <- &rpcReply{success: success, reply: reply}
}

// wait for asyn call
func waitCall(replyChan chan *rpcReply, numCall int) {
	for i := 0; i < numCall; i++ {
		<-replyChan
	}
}

// recover replaces a new node with a dead one
func (c *Consul) recover() {
	numAliveNodes := len(c.aliveAppServersRpc)
	replyChan := make(chan *rpcReply, numAliveNodes)

	// pause all alive servers
	pauseReplies := make([]*appserverrpc.PauseReply, numAliveNodes)
	for i, srv := range c.aliveAppServersRpc {
		pauseReplies[i] = new(appserverrpc.PauseReply)
		go asyncCall(srv, "AppServer.Pause", new(appserverrpc.PauseArgs), pauseReplies[i], replyChan)
	}
	waitCall(replyChan, numAliveNodes)

	maxSlot := -1
	for _, reply := range pauseReplies {
		if reply.MaxSlot > maxSlot {
			maxSlot = reply.MaxSlot
		}
	}

	// copy state from one replica
	replicaAddr := c.aliveAppServersRpc[numAliveNodes-1]
	copyStateReply := new(appserverrpc.CopyStateReply)
	call(replicaAddr, "AppServer.CopyState", new(appserverrpc.CopyStateArgs), copyStateReply)

	// notify new server to update state
	updateArgs := new(appserverrpc.UpdateStateArgs)
	updateArgs.Max = copyStateReply.Max
	updateArgs.Min = copyStateReply.Min
	updateArgs.MaxSlot = maxSlot
	updateArgs.Instances = copyStateReply.Instances

	call(c.newAppServerRpc, "AppServer.UpdateState", updateArgs, new(appserverrpc.UpdateStateReply))

	// notify old servers to replace server address
	replaceArgs := &appserverrpc.ReplaceArgs{
		DeadAppServer:    c.deadAppServer,
		DeadAppServerRpc: c.deadAppServerRpc,
		DeadPaxosServer:  c.deadPaxosServer,
		DeadOTServer:     c.deadOTServer,
		NewAppServer:     c.newAppServer,
		NewAppServerRpc:  c.newAppServerRpc,
		NewPaxosServer:   c.newPaxosServer,
		NewOTServer:      c.newOTServer,
	}

	for _, srv := range c.aliveAppServersRpc {
		go asyncCall(srv, "AppServer.Replace", replaceArgs, new(appserverrpc.ReplaceReply), replyChan)
	}
	waitCall(replyChan, numAliveNodes)

	// notify all servers to restart
	for _, srv := range c.aliveAppServersRpc {
		go asyncCall(srv, "AppServer.Restart", new(appserverrpc.RestartArgs), new(appserverrpc.RestartReply), replyChan)
	}
	waitCall(replyChan, numAliveNodes)
}

/*********************
 * Runner
 **********************/
func main() {
	config := flag.String("config", "", "config file about server list")

	flag.Parse()
	consul := NewConsul(*config)
	consul.recover()
}
