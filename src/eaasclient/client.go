package main

import (
	"bufio"
	"github.com/copilot/src/dlog"
	"flag"
	"fmt"
	"github.com/copilot/src/genericsmrproto"
	"log"
	"github.com/copilot/src/masterproto"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"github.com/copilot/src/state"
	"time"
  "github.com/EaaS"
)

const REQUEST_TIMEOUT = 100 * time.Millisecond
const GET_VIEW_TIMEOUT = 100 * time.Millisecond
const GC_DEBUG_ENABLED = false
const PRINT_STATS = false

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var twoLeaders *bool = flag.Bool("twoLeaders", true, "Two leaders for slowdown tolerance. Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", 0, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var cid *int = flag.Int("id", -1, "Client ID.")
var cpuProfile *string = flag.String("cpuprofile", "", "Name of file for CPU profile. If empty, no profile is created.")
var maxRuntime *int = flag.Int("runtime", -1, "Max duration to run experiment in second. If negative, stop after sending up to reqsNb requests")

//var debug *bool = flag.Bool("debug", false, "Enable debug output.")
var trim *float64 = flag.Float64("trim", 0.25, "Exclude some fraction of data at the beginning and at the end.")
var prefix *string = flag.String("prefix", "", "Path prefix for filenames.")
var hook *bool = flag.Bool("hook", true, "Add shutdown hook.")
var verbose *bool = flag.Bool("verbose", true, "Print throughput to stdout.")
var numKeys *uint64 = flag.Uint64("numKeys", 100000, "Number of keys in simulated store.")
var proxyReplica *int = flag.Int("proxy", -1, "Replica Id to proxy requests to. If id < 0, use request Id mod N as default.")
var sendOnce *bool = flag.Bool("once", false, "Send request to only one leader.")
var tput_interval *float64 = flag.Float64("tput_interval_in_sec", 1, "Time interval to record and print throughput")

// GC debug
var garPercent = flag.Int("garC", 50, "Collect info about GC")

var N int

var clientId uint32

var successful []int
var rsp []bool

// var rarray []int

var latencies []int64
var readlatencies []int64
var writelatencies []int64

var timestamps []time.Time

type DataPoint struct {
	elapse    time.Duration
	reqsCount int64
	t         time.Time
}

type Response struct {
	OpId       int32
	rcvingTime time.Time
	timestamp  int64
  Value state.Value
}

type View struct {
	ViewId    int32
	PilotId   int32
	ReplicaId int32
	Active    bool
}

var throughputs []DataPoint

/*
  Variables taken out of main and made global for EAAS
*/
var views []*View
var leader int 
var leader2 int 
var readers []*bufio.Reader
var writers []*bufio.Writer
var leaderReplyChan chan int32
var pilot0ReplyChan chan Response
var viewChangeChan chan *View
var reqsCount int64 = 0
var isRandomLeader bool
var reqNum int = 0
/*
  End of variables put to global for EAAS
*/

func main() {
  StartClient()
  result := make([]int32, 2)
  values := make([]int32, 2)
  values[0] = 92
  Put(6, nil, values, 0)
  Get(6, nil, 0, result)
  fmt.Println("Get Result for key 6: expected: 92, actual: ", result[0])
  values[0] = 37
  Put(7, nil, values, 0)
  Get(7, nil, 0, result)
  fmt.Println("Get Result for key 7: expected: 37, actual: ", result[0])
  Get(6, nil, 0, result)
  fmt.Println("Get Result for key 6: expected: 92, actual: ", result[0])
}

//func main() {
func StartClient() {

	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			dlog.Printf("Error creating CPU profile file %s: %v\n", *cpuProfile, err)
		}
		pprof.StartCPUProfile(f)
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt)
		go catchKill(interrupt)
		defer pprof.StopCPUProfile()
	}

	if *hook {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go shutdownHook(c)
	}

	if *cid < 0 {
		clientId = generateRandomClientId()
	} else {
		clientId = uint32(*cid)
	}

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers = make([]*bufio.Reader, N)
	writers = make([]*bufio.Writer, N)

	if *conflicts >= 0 {
		fmt.Println("Uniform distribution")
	} else {
		fmt.Println("Zipfian distribution:")
	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])

	}

	if *twoLeaders {
		fmt.Println("Registering client id", clientId)
		/* Register Client Id */
		for i := 0; i < N; i++ {
			rciArgs := &genericsmrproto.RegisterClientIdArgs{ClientId: clientId}
			writers[i].WriteByte(genericsmrproto.REGISTER_CLIENT_ID)
			rciArgs.Marshal(writers[i])
			writers[i].Flush()
		}
	}

	time.Sleep(5 * time.Second)

	successful = make([]int, N)
	leader = -1

	// second leader
	leader2 = -1

	isRandomLeader = false

	if *noLeader == false {

		if *twoLeaders == false {
			reply := new(masterproto.GetLeaderReply)
			if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
				log.Fatalf("Error making the GetLeader RPC\n")
			}
			leader = reply.LeaderId
			log.Printf("The leader is replica %d\n", leader)
		} else { // two leaders
			reply := new(masterproto.GetTwoLeadersReply)

			if err = master.Call("Master.GetTwoLeaders", new(masterproto.GetTwoLeadersArgs), reply); err != nil {
				log.Fatalf("Error making the GetTwoLeaders")
			}
			leader = reply.Leader1Id
			leader2 = reply.Leader2Id
			//fmt.Printf("The leader 1 is replica %d. The leader 2 is replica %d\n", leader, leader2)
			fmt.Printf("The leader 1 is replica %d (%s). The leader 2 is replica %d (%s)\n", leader, rlReply.ReplicaList[leader], leader2, rlReply.ReplicaList[leader2])

			// Init views. Assume initial view id is 0
			views = make([]*View, 2)
			views[0] = &View{ViewId: 0, PilotId: 0, ReplicaId: int32(leader), Active: true}
			views[1] = &View{ViewId: 0, PilotId: 1, ReplicaId: int32(leader2), Active: true}

		}
	} else if *proxyReplica >= 0 && *proxyReplica < N {
		leader = *proxyReplica
	} else { // epaxos and no designated proxy specified
		isRandomLeader = true
	}

	if *check {
		rsp = make([]bool, *reqsNb)
		for j := 0; j < *reqsNb; j++ {
			rsp[j] = false
		}
	}

	//var done chan bool

  pilot0ReplyChan = make(chan Response, *reqsNb)
  viewChangeChan = make(chan *View, 100)
  for i := 0; i < N; i++ {
    go waitRepliesPilot(readers, i, pilot0ReplyChan, viewChangeChan, *reqsNb*2)
  }


}

/* Get(key, columns[], numColumns, results[]) */
func Get(key int64, _ []int32, _ int, result []int32) int{
  fmt.Println("In Get")
    var pilotErr, pilotErr1 error
    var lastGVSent0, lastGVSent1 time.Time
    id := int32(reqNum)
    reqNum += 1
		args := genericsmrproto.Propose{id, state.Command{ClientId: clientId, OpId: id, Op: state.GET, K: state.Key(key), V: 0}, time.Now().UnixNano()}

		/* Prepare proposal */
		dlog.Printf("Sending proposal %d\n", id)

		before := time.Now()
		timestamps = append(timestamps, before)

		repliedCmdId := int32(-1)
		var to *time.Timer
		succeeded := false
		if *twoLeaders {
			for {
				// Check if there is newer view
				for i := 0; i < len(viewChangeChan); i++ {
					newView := <-viewChangeChan
					if newView.ViewId > views[newView.PilotId].ViewId {
						views[newView.PilotId].PilotId = newView.PilotId
						views[newView.PilotId].ReplicaId = newView.ReplicaId
						views[newView.PilotId].ViewId = newView.ViewId
						views[newView.PilotId].Active = true
					}
				}

				// get random server to ask about new view
				serverId := rand.Intn(N)
				if views[0].Active {
					leader = int(views[0].ReplicaId)
					pilotErr = nil
					if leader >= 0 {
						writers[leader].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader])
						pilotErr = writers[leader].Flush()
						if pilotErr != nil {
							views[0].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[0].Active {
					leader = -1
					if lastGVSent0 == (time.Time{}) || time.Since(lastGVSent0) >= GET_VIEW_TIMEOUT {
						for ; serverId == 0; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{0}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent0 = time.Now()
					}
				}

				if views[1].Active {
					leader2 = int(views[1].ReplicaId)
					/* Send to second leader for two-leader protocol */
					pilotErr1 = nil
					if *twoLeaders && !*sendOnce && leader2 >= 0 {
						writers[leader2].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader2])
						pilotErr1 = writers[leader2].Flush()
						if pilotErr1 != nil {
							views[1].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[1].Active {
					leader2 = -1
					if lastGVSent1 == (time.Time{}) || time.Since(lastGVSent1) >= GET_VIEW_TIMEOUT {
						for ; serverId == 1; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{1}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent1 = time.Now()
					}
				}
				if !succeeded {
					continue
				}

				// we successfully sent to at least one pilot
				succeeded = false
				to = time.NewTimer(REQUEST_TIMEOUT)
				toFired := false
				for true {
					select {
					case e := <-pilot0ReplyChan:
						repliedCmdId = e.OpId
						if repliedCmdId == id {
							to.Stop()
							succeeded = true
						}
//            fmt.Println("Value in Get: ", e.Value)
            result[0] = int32(e.Value)

					case <-to.C:
						fmt.Printf("Client %v: TIMEOUT for request %v\n", clientId, id)
						repliedCmdId = -1
						succeeded = false
						toFired = true

					default:
					}

					if succeeded {
						if *check {
							rsp[id] = true
						}
						reqsCount++
						break
					} else if toFired {
						break
					}
				} // end of foor loop waiting for result
				// successfully get the response. continue with the next request
				if succeeded {
					break
				} else if toFired {
					continue
				}
			} // end of copilot
		} 

    return EaaS.EAAS_W_EC_SUCCESS
}

/* Put(key, columns[], values[], size) */
func Put(key int64, _ []int32, values []int32, _ int) int {
    fmt.Println("In PUT")
    var pilotErr, pilotErr1 error
    var lastGVSent0, lastGVSent1 time.Time
    id := int32(reqNum)
    reqNum += 1
		args := genericsmrproto.Propose{id, state.Command{ClientId: clientId, OpId: id, Op: state.PUT, K: state.Key(key), V: state.Value(values[0])}, time.Now().UnixNano()}

		/* Prepare proposal */
		dlog.Printf("Sending proposal %d\n", id)

		before := time.Now()
		timestamps = append(timestamps, before)

		repliedCmdId := int32(-1)
		var to *time.Timer
		succeeded := false
		if *twoLeaders {
			for {
				// Check if there is newer view
				for i := 0; i < len(viewChangeChan); i++ {
					newView := <-viewChangeChan
					if newView.ViewId > views[newView.PilotId].ViewId {
						views[newView.PilotId].PilotId = newView.PilotId
						views[newView.PilotId].ReplicaId = newView.ReplicaId
						views[newView.PilotId].ViewId = newView.ViewId
						views[newView.PilotId].Active = true
					}
				}

				// get random server to ask about new view
				serverId := rand.Intn(N)
				if views[0].Active {
					leader = int(views[0].ReplicaId)
					pilotErr = nil
					if leader >= 0 {
						writers[leader].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader])
						pilotErr = writers[leader].Flush()
						if pilotErr != nil {
							views[0].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[0].Active {
					leader = -1
					if lastGVSent0 == (time.Time{}) || time.Since(lastGVSent0) >= GET_VIEW_TIMEOUT {
						for ; serverId == 0; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{0}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent0 = time.Now()
					}
				}

				if views[1].Active {
					leader2 = int(views[1].ReplicaId)
					/* Send to second leader for two-leader protocol */
					pilotErr1 = nil
					if *twoLeaders && !*sendOnce && leader2 >= 0 {
						writers[leader2].WriteByte(genericsmrproto.PROPOSE)
						args.Marshal(writers[leader2])
						pilotErr1 = writers[leader2].Flush()
						if pilotErr1 != nil {
							views[1].Active = false
						} else {
							succeeded = true
						}
					}
				}
				if !views[1].Active {
					leader2 = -1
					if lastGVSent1 == (time.Time{}) || time.Since(lastGVSent1) >= GET_VIEW_TIMEOUT {
						for ; serverId == 1; serverId = rand.Intn(N) {
						}
						getViewArgs := &genericsmrproto.GetView{1}
						writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
						getViewArgs.Marshal(writers[serverId])
						writers[serverId].Flush()
						lastGVSent1 = time.Now()
					}
				}
				if !succeeded {
					continue
				}

				// we successfully sent to at least one pilot
				succeeded = false
				to = time.NewTimer(REQUEST_TIMEOUT)
				toFired := false
				for true {
					select {
					case e := <-pilot0ReplyChan:
						repliedCmdId = e.OpId
						if repliedCmdId == id {
							to.Stop()
							succeeded = true
						}
//            fmt.Println("Value in Put ", e.Value)

					case <-to.C:
						fmt.Printf("Client %v: TIMEOUT for request %v\n", clientId, id)
						repliedCmdId = -1
						succeeded = false
						toFired = true

					default:
					}

					if succeeded {
						if *check {
							rsp[id] = true
						}
						reqsCount++
						break
					} else if toFired {
						break
					}
				} // end of foor loop waiting for result
				// successfully get the response. continue with the next request
				if succeeded {
					break
				} else if toFired {
					continue
				}
			} // end of copilot
		} 

    return 0
}

func waitRepliesPilot(readers []*bufio.Reader, leader int, done chan Response, viewChangeChan chan *View, expected int) {

	var msgType byte
	var err error

	reply := new(genericsmrproto.ProposeReplyTS)
	getViewReply := new(genericsmrproto.GetViewReply)
	for true {
		if msgType, err = readers[leader].ReadByte(); err != nil {
			break
		}

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if reply.OK != 0 {
				successful[leader]++
				done <- Response{reply.CommandId, time.Now(), reply.Timestamp, reply.Value}
				if expected == successful[leader] {
					return
				}
			}
			break

		case genericsmrproto.GET_VIEW_REPLY:
			if err = getViewReply.Unmarshal(readers[leader]); err != nil {
				break
			}
			if getViewReply.OK != 0 { /*View is active*/
				viewChangeChan <- &View{getViewReply.ViewId, getViewReply.PilotId, getViewReply.ReplicaId, true}
			}
			break

		default:
			break
		}
	}

}

func generateRandomClientId() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Uint32()
}

func generateRandomOpId() int32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Int31()
}

func printer(dataChan chan *DataPoint, done chan bool) {
	for {
		reading, more := <-dataChan
		if !more {
			if done != nil {
				done <- true
			}
			return
		}
		fmt.Printf("%.1f\t%d\t%.0f\n", float64(reading.elapse)/float64(time.Second), reading.reqsCount, float64(reading.reqsCount)*float64(time.Second)/float64(reading.elapse))
	}

}



func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
	}
	dlog.Printf("Caught signal and stopped CPU profile before exit.\n")
	os.Exit(0)
}

/* Helper functions to write to file */
func checkError(e error) {
	if e != nil {
		panic(e)
	}
}


func shutdownHook(c chan os.Signal) {
	sig := <-c
	fmt.Printf("I've got killed by signal %s! Cleaning up...", sig)

	os.Exit(1)
}


/* Helper interface for sorting int64 */
type int64Slice []int64

func (arr int64Slice) Len() int {
	return len(arr)
}

func (arr int64Slice) Less(i, j int) bool {
	return arr[i] < arr[j]
}

func (arr int64Slice) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}
