package main

import (
	"container/list"
	"cs.cmu.edu/mcds/paxos"
	"cs.cmu.edu/mcds/rpc/appserverrpc"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	LOGE = log.New(os.Stderr, "ERROR ", log.Lmicroseconds|log.Lshortfile)
	LOGV = log.New(os.Stdout, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
	//LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
)

type cmd int

const (
	NormalOp cmd = iota + 1
	Noop
	GetDoc
)

const (
	TIMEOUTLIMITS = 10
)

type Value struct {
	Cmd cmd    // 2: GetDoc 1: nop, 0: normal op
	Seq string // sequence id that uniquely
	Doc string // updated document
}

func (app *AppServer) newNormalOp(doc string) Value {
	return Value{Cmd: NormalOp, Doc: doc, Seq: app.genSeqId()}
}

func (app *AppServer) newNoOp() Value {
	return Value{Cmd: Noop, Doc: "", Seq: app.genSeqId()}
}

func (app *AppServer) newGetDocValue() Value {
	return Value{Cmd: GetDoc, Doc: "", Seq: app.genSeqId()}
}

func (app *AppServer) genSeqId() string {
	t := strconv.FormatInt(time.Now().UnixNano(), 10)
	return app.meAppAddr + t
}

// serialize the value into a value
func (v Value) Serialize() interface{} {
	s, _ := json.Marshal(v)
	return s
}

func Deserialize(bs []byte) Value {
	var v Value
	err := json.Unmarshal(bs, &v)
	if err != nil {
		LOGE.Println("unmarshall error")
	}
	return v
}

////////////////////
// Web Service
////////////////////

// return index page
func (app *AppServer) handleIndex(response http.ResponseWriter, request *http.Request) {
	response.Write([]byte("hello world"))
}

// execute a new instruction

// Parameters:
// SeqNum : the command slot
// Value : the command we want to execute

// Return:
// 200 : executed successfully
// 400 : error
func (app *AppServer) handleNewInstr(response http.ResponseWriter, request *http.Request) {
	seqNum := request.FormValue("seq")
	op := request.FormValue("op")
	doc := request.FormValue("doc")
	if len(seqNum) > 0 && len(op) > 0 {
		// really do the work
		LOGV.Println("New Instr Request: ", "seq: ", seqNum, " op: ", op, " doc : ", doc)
		value := app.newNormalOp(doc)

		if app.isPaused() {
			app.waitForRestart()
		}

		ok := app.proposeUntilAccepted(value)

		if ok {
			LOGV.Println("New instr succeed.")
			response.WriteHeader(http.StatusAccepted)
			response.Write([]byte("aa"))

		} else {
			LOGE.Println("Time out for new instruction, might happen")
			response.WriteHeader(http.StatusBadRequest)
		}
	} else {
		response.WriteHeader(http.StatusBadRequest)
	}
}

func (app *AppServer) proposeUntilAccepted(val Value) bool {
	slot := app.paxos.Max()
	v := val.Serialize()

	for {
		decided := false
		slot += 1
		app.paxos.Start(slot, v)
		LOGV.Println("propose in slot", slot, val)
		app.proposals[slot] = true

		for i := 0; i < 10; i += 1 {
			select {
			case <-time.After(TIMEOUTLIMITS * time.Millisecond):
				ok, t := app.paxos.Status(slot)
				if ok {
					decidedVal := Deserialize(t.([]byte))
					// chosen
					if decidedVal.Seq == val.Seq {
						LOGV.Println(val, " chosen ins lot", slot)
						return true
					} else {
						decided = true
						break
					}
				}
			}
		}

		// timeout for proposing a value
		if !decided {
			return false
		}
	}
}

// get most update-to-date appserver list

// Return:
// List of AppServer HostPort
func (app *AppServer) handleGetServerList(response http.ResponseWriter, request *http.Request) {
	s := ""
	for _, srv := range app.otServers {
		s += srv
		s += " "
	}
	response.Write([]byte(s))
}

// get most update to date doc

// Parameter:
// docId : document id

// Return:
// full document
func (app *AppServer) handleGetDoc(response http.ResponseWriter, request *http.Request) {
	doc, err := app.getMostRecentDoc()
	if err == nil {
		response.Write(encodeDocResponse(app.paxos.Max(), doc))
	} else {
		LOGE.Println("Time out for new instruction, might happen")
		response.WriteHeader(http.StatusBadRequest)
	}
}

// propose GetDoc, and get the most recent slot
func (app *AppServer) getMostRecentDoc() (string, error) {

	// fill the empty slot
	for app.paxos.Min() <= app.paxos.Max() {
		select {
		case <-time.After(TIMEOUTLIMITS * time.Millisecond):
			for slot := app.paxos.Min(); slot <= app.paxos.Max(); slot++ {
				if _, present := app.proposals[slot]; !present {
					noop := app.newNoOp()
					t := noop.Serialize()
					app.paxos.Start(slot, t)
					app.proposals[slot] = true
				}
			}
		}
	}

	// propose getDoc until accepted
	val := app.newGetDocValue()
	ok := app.proposeUntilAccepted(val)

	if !ok {
		return "", errors.New("time out proposing get doc")
	}

	// find the most recent slot that contains the most recent doc
	doc := ""

	for slot := app.paxos.Min() - 1; slot > 0; slot-- {
		_, bytes := app.paxos.Status(slot)
		v := Deserialize(bytes.([]byte))
		if v.Cmd == NormalOp {
			doc = v.Doc
			break
		}
	}

	return doc, nil

}

func encodeDocResponse(seq int, doc string) []byte {
	s := fmt.Sprintf("{\"seq\":%v, \"doc\":\"%v\"}", seq, doc)
	LOGV.Println("encode response to :", s)
	return []byte(s)
}

func (app AppServer) wait(seq int, r chan bool) {
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			ok, _ := app.paxos.Status(seq)
			if ok {
				r <- true
				return
			}
		}
	}
	return
}

// heartbeat check liveness

// Return:
// 200: OK
// else: error
func (app *AppServer) handleHeartBeat(response http.ResponseWriter, request *http.Request) {
	response.Write([]byte("heartbeat"))
}

////////////////////
// initialization
////////////////////

type AppServer struct {
	started         chan bool
	me              int
	mePaxosAddr     string
	meAppAddr       string
	meAppRpcAddr    string
	meOTServer      string
	config          string
	paxosServers    []string
	appServers      []string
	appServersRpc   []string
	otServers       []string
	proposals       map[int]bool
	paused          bool
	pendingRequests *list.List
	mu              *sync.Mutex
	paxos           paxos.Paxos
}

func (app *AppServer) initServer() {
	app.startPaxos()
	app.registerRpc()
	app.started <- true
}

// start paxos peer in this server
func (app *AppServer) startPaxos() {
	// read server list
	b, err := ioutil.ReadFile(app.config)
	if err != nil {
		panic(err)
	}
	s := string(b)
	lines := strings.Split(s, "\n")
	app.paxosServers = strings.Split(lines[0], " ")
	app.appServers = strings.Split(lines[1], " ")
	app.appServersRpc = strings.Split(lines[2], " ")
	app.otServers = strings.Split(lines[3], " ")
	app.mePaxosAddr = app.paxosServers[app.me]
	app.meAppAddr = app.appServers[app.me]
	app.meAppRpcAddr = app.appServersRpc[app.me]
	app.meOTServer = app.otServers[app.me]

	app.info()
	// start paxos
	app.paxos = paxos.Make(app.paxosServers, app.me, nil)
	LOGV.Println("Paxos peer started ")
}

func (app *AppServer) registerRpc() {
	err := rpc.Register(appserverrpc.Wrap(app))
	if err != nil {
		panic(err)
	}
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", app.meAppRpcAddr)
	if e != nil {
		panic(e)
	}
	go http.Serve(l, nil)

	LOGV.Println("AppServer RPC registered")
}

func (app *AppServer) isPaused() bool {
	app.mu.Lock()
	defer app.mu.Unlock()

	return app.paused
}

func (app *AppServer) waitForRestart() {
	pendingChan := make(chan interface{})
	app.mu.Lock()
	app.pendingRequests.PushBack(pendingChan)
	app.mu.Unlock()
	<-pendingChan
}

func (app *AppServer) info() {
	LOGV.Println("Paxos server list is:", app.paxosServers)
	LOGV.Println("App server list is:", app.appServers)
	LOGV.Println("App server rpc list is:", app.appServersRpc)
	LOGV.Println("OT server list is:", app.otServers)
	LOGV.Println("Current app server addr is: ", app.meAppAddr)
	LOGV.Println("Current paxos server addr is: ", app.mePaxosAddr)
	LOGV.Println("Current app server rpc addr is: ", app.meAppRpcAddr)
	LOGV.Println("Current OT server: ", app.meOTServer)
}

// helper function, replace oldStr in arr with newStr
func replace(arr []string, oldsStr, newStr string) []string {
	for i := range arr {
		if arr[i] == oldsStr {
			arr[i] = newStr
		}
	}

	return arr
}

/*********************
 * RPCs
 **********************/
// Pause pauses the app server, stop it from handling new instructions
// also performs catch up to have all servers reach to the same state
func (app *AppServer) Pause(args *appserverrpc.PauseArgs, reply *appserverrpc.PauseReply) error {
	LOGV.Println(app.meAppRpcAddr, " received Pause")
	app.mu.Lock()
	app.paused = true
	app.mu.Unlock()

	// fill all empty slots in the gap
	// propose no-op if the current server does not act as a proposer
	for app.paxos.Min() <= app.paxos.Max() {
		LOGV.Println(app.paxos.Min(), app.paxos.Max())
		select {
		case <-time.After(10 * time.Millisecond):
			for slot := app.paxos.Min(); slot <= app.paxos.Max(); slot++ {
				if _, present := app.proposals[slot]; !present {
					value := app.newNoOp()
					t := value.Serialize()
					app.paxos.Start(slot, t)
					app.proposals[slot] = true
				}
			}
		}
	}

	reply.MaxSlot = app.paxos.Max()

	return nil
}

func (app *AppServer) UpdateState(args *appserverrpc.UpdateStateArgs, reply *appserverrpc.UpdateStateReply) error {
	LOGV.Println(app.meAppRpcAddr, " received UpdateState")
	app.paxos.SetState(args.Min, args.Max, args.MaxSlot, args.Instances)
	app.paxos.Logging()
	return nil
}

func (app *AppServer) CopyState(args *appserverrpc.CopyStateArgs, reply *appserverrpc.CopyStateReply) error {
	LOGV.Println(app.meAppRpcAddr, " received CopyState")
	defer LOGV.Println("Copy State complete")
	min, max, instances := app.paxos.GetState()
	reply.Min = min
	reply.Max = max
	reply.Instances = instances

	return nil
}

func (app *AppServer) Replace(args *appserverrpc.ReplaceArgs, reply *appserverrpc.ReplaceArgs) error {
	LOGV.Println(app.meAppRpcAddr, " received Replace")
	app.appServers = replace(app.appServers, args.DeadAppServer, args.NewAppServer)
	app.appServersRpc = replace(app.appServersRpc, args.DeadAppServerRpc, args.NewAppServerRpc)
	app.paxosServers = replace(app.paxosServers, args.DeadPaxosServer, args.NewPaxosServer)
	app.otServers = replace(app.otServers, args.DeadOTServer, args.NewOTServer)
	app.info()

	return nil
}

func (app *AppServer) Restart(args *appserverrpc.RestartArgs, reply *appserverrpc.RestartReply) error {
	LOGV.Println(app.meAppRpcAddr, " received Restart")
	app.mu.Lock()
	defer app.mu.Unlock()

	app.paused = false
	for e := app.pendingRequests.Front(); e != nil; e = e.Next() {
		channel := e.Value.(chan interface{})
		channel <- struct{}{}
	}

	app.paxos.Logging()
	return nil
}

/*********************
 * Runner
 **********************/
func main() {
	paxosServerList := flag.String("config", "", "config file about paxos server list")
	me := flag.Int("me", -1, "current paxos number")

	flag.Parse()

	if len(*paxosServerList) <= 0 || *me < 0 {
		LOGV.Println("incorrect option")
		os.Exit(-1)
	}

	LOGV.Println("Config file is :", *paxosServerList)

	app := &AppServer{
		started:         make(chan bool),
		me:              *me,
		config:          *paxosServerList,
		proposals:       make(map[int]bool),
		pendingRequests: list.New(),
		paused:          false,
		mu:              new(sync.Mutex),
	}

	// main entry
	http.HandleFunc("/", app.handleIndex)
	http.HandleFunc("/new_instr", app.handleNewInstr)
	http.HandleFunc("/get_serverlist", app.handleGetServerList)
	http.HandleFunc("/get_doc", app.handleGetDoc)
	http.HandleFunc("/heartbeat", app.handleHeartBeat)

	go app.initServer()
	<-app.started
	LOGV.Println("AppServer Started")
	defer app.paxos.Logging()
	http.ListenAndServe(app.meAppAddr, nil)
}
