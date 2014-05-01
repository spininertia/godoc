/*********************
 * Test for basic Paxos
 *
 * Siping Ji (sipingji@cmu.edu)
 * Wei Chen  (weichen1@andrew.cmu.edu)
 *********************/

package paxos

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

var LOGT = log.New(os.Stdout, "State ", log.Lmicroseconds|log.Lshortfile)

var portList = [...]int{40000, 40001, 40002, 40003, 40004, 40005, 40007, 40008, 40009, 40010}

/****************************
 * Test Helper Functions
 ****************************/

// Generate Server Address for paxos peer
func genServerAddr(i int) string {
	srv := "localhost:"
	srv += strconv.Itoa(portList[i])
	return srv
}

// pseudo Paxos instance generator
func randPaxos(num int) (paxos []Paxos, ports []string) {
	var i int
	paxos = make([]Paxos, num)
	ports = make([]string, num)
	for i = 0; i < num; i += 1 {
		ports[i] = genServerAddr(i)
	}

	for i = 0; i < num; i += 1 {
		paxos[i] = Make(ports, i, nil)
	}
	return
}

// paxoseudo Value generator
func randValues(num int) (msgs []string) {
	msgs = make([]string, num)
	for i := 0; i < num; i += 1 {
		val := rand.Int()
		for val == 0 {
			val = rand.Int()
		}
		msgs[i] = strconv.Itoa(val)
	}
	return
}

// Count number of instance votes
func countValue(t testing.TB, paxos []Paxos, seq int) int {
	count := 0
	var prev interface{}
	prev = nil
	for i, px := range paxos {
		if px != nil {
			ok, currVal := px.Status(seq)
			if ok {
				if count > 0 && prev != nil && currVal != nil && prev != currVal {
					t.Fatalf("decided values do not match; seq=%v i=%v currVal=%v prev=%v",
						seq, i, prev, currVal)
				}
				count++
				prev = currVal
			}
		}
	}
	return count
}

// Wait required number of votes
func waitDecidedPeer(t testing.TB, paxos []Paxos, seq int, wanted int) {
	sleepTime := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if countValue(t, paxos, seq) >= wanted {
			break
		}
		time.Sleep(sleepTime)
		if sleepTime < time.Second {
			sleepTime *= 2
		}
	}
	nd := countValue(t, paxos, seq)
	if nd < wanted {
		t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
	}
}

// test whether a single value is chosen
func timeoutWait(t testing.TB, paxos []Paxos, seq int) bool {
	sleepTime := 10 * time.Millisecond
	for iters := 0; iters < 10; iters++ {
		if countValue(t, paxos, seq) > 0 {
			break
		}
		time.Sleep(sleepTime)
		if sleepTime < time.Second {
			sleepTime *= 2
		}
	}
	nd := countValue(t, paxos, seq)
	if nd > 0 {
		return false
	}

	return true
}

// Generate network partitions among paxos peers
func genPartition(paxos []Paxos, p1 []int, p2 []int, p3 []int) {
	if p1 == nil {
		for _, px := range paxos {
			px.SetPartition(nil)
		}

		return
	}

	pa := [][]int{p1, p2, p3}
	for i := 0; i < len(pa); i++ {
		p := pa[i]
		for j := 0; j < len(p); j++ {
			paxos[p[j]].SetPartition(p)
		}
	}
}

// Shut down set of Paxos
func ShutDown(paxos []Paxos) {
	for _, p := range paxos {
		p.Kill()
	}
	time.Sleep(time.Second)
}

/********************
// Tests
********************/

// Perfect network

// Only proposed value should be chosen
func TestSingleProposer(t *testing.T) {
	fmt.Println("Test: Single proposer, Perfect Network")
	paxos, _ := randPaxos(5)
	defer ShutDown(paxos)

	msgs := randValues(50)
	for i, v := range msgs {
		paxos[0].Start(i, v)
		waitDecidedPeer(t, paxos, i, 5)
	}

	for i, v := range msgs {
		ok, vv := paxos[0].Status(i)
		if !ok || vv.(string) != v {
			t.Fatal("Value not match!", v, " ", vv)
			return
		}
	}
	fmt.Println("  Passed!")
}

func TestMultipleProposersForDifferentSlot(t *testing.T) {
	fmt.Println("Test: Multiple Proposers in different slots, perfect networks")
	numPaxos := 3

	paxos, _ := randPaxos(numPaxos)
	defer ShutDown(paxos)

	numSlot := 10
	totalSlot := numSlot * numPaxos

	msgs := randValues(totalSlot)

	for slot := 0; slot < totalSlot; slot++ {
		paxos[slot%numPaxos].Start(slot, msgs[slot])
	}

	for i := 0; i < numSlot; i++ {
		waitDecidedPeer(t, paxos, i, numPaxos)
		_, val := paxos[0].Status(i)
		if val != msgs[i] {
			t.Fatal("Value chosen is not from a single proposer!")
		}
	}

	fmt.Println("  Passed!")
}

// Only one  value should be chosen
// the one value should be proposed by some paxos peer
func TestMultipleProposersInSameSlot(t *testing.T) {
	fmt.Println("Multiple Proposers in the same slot, perfect networks")
	numPaxos := 3

	paxos, _ := randPaxos(numPaxos)
	defer ShutDown(paxos)

	msgs := make([][]string, numPaxos)

	numSlot := 30

	for i := range msgs {
		msgs[i] = randValues(numSlot)
	}

	for i := 0; i < numSlot; i++ {
		for j := 0; j < numPaxos; j++ {
			paxos[j].Start(i, msgs[j][i])
		}
		time.Sleep(15 * time.Millisecond)
	}

	for i := 0; i < numSlot; i++ {
		waitDecidedPeer(t, paxos, i, numPaxos)
		_, val := paxos[0].Status(i)
		flag := false

		for j := 0; j < numPaxos; j++ {
			if msgs[j][i] == val {
				flag = true
				break
			}
		}

		if !flag {
			t.Fatal("Value chosen is not from a single proposer!")
		}
	}

	fmt.Println("  Passed!")
}

// Network partition

func TestPartition(t *testing.T) {
	// runtime.GOMAXPROCS(4)

	const npaxos = 5
	var paxos []Paxos = make([]Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer ShutDown(paxos)

	for i := 0; i < npaxos; i++ {
		pxh[i] = genServerAddr(i)
	}

	for i := 0; i < npaxos; i++ {
		paxos[i] = Make(pxh, i, nil)
	}

	defer genPartition(paxos, nil, nil, nil)

	seq := 0

	fmt.Println("Test: No partition contains majority, no agreement should be reached")

	genPartition(paxos, []int{0, 2}, []int{1, 3}, []int{4})
	paxos[1].Start(seq, 111)

	if !timeoutWait(t, paxos, seq) {
		t.Fatal("Reach agreement when majority failed")
	}

	fmt.Println("  Passed")

	fmt.Println("Test: Reach agreement when one partition contains majority")

	genPartition(paxos, []int{0}, []int{1, 2, 3}, []int{4})
	time.Sleep(2 * time.Second)
	waitDecidedPeer(t, paxos, seq, npaxos/2+1)

	fmt.Println("  Passed")

	fmt.Println("Test: All replica reaches consistent state after partition disappears")

	paxos[0].Start(seq, 1000)
	paxos[4].Start(seq, 1004)
	genPartition(paxos, []int{0, 1, 2, 3, 4}, []int{}, []int{})

	waitDecidedPeer(t, paxos, seq, npaxos)

	fmt.Println("  Passed")

	fmt.Println("Test: One replica switches partition, perfect network")

	for iters := 0; iters < 20; iters++ {
		seq++

		genPartition(paxos, []int{0, 1, 2}, []int{3, 4}, []int{})
		paxos[0].Start(seq, seq*10)
		paxos[3].Start(seq, (seq*10)+1)
		waitDecidedPeer(t, paxos, seq, npaxos/2+1)
		if countValue(t, paxos, seq) > 3 {
			fmt.Println(countValue(t, paxos, seq))
			t.Fatalf("too many decided")
		}

		genPartition(paxos, []int{0, 1}, []int{2, 3, 4}, []int{})
		waitDecidedPeer(t, paxos, seq, npaxos)
	}

	fmt.Println("  Passed")

	fmt.Println("Test: One replica switches partition, unreliable network")

	for iters := 0; iters < 20; iters++ {
		seq++

		for i := 0; i < npaxos; i++ {
			paxos[i].SetUnreliable(true)
		}

		genPartition(paxos, []int{0, 1, 2}, []int{3, 4}, []int{})
		for i := 0; i < npaxos; i++ {
			paxos[i].Start(seq, (seq*10)+i)
		}

		waitDecidedPeer(t, paxos, seq, 3)
		if countValue(t, paxos, seq) > 3 {
			t.Fatalf("too many decided")
		}

		genPartition(paxos, []int{0, 1}, []int{2, 3, 4}, []int{})

		for i := 0; i < npaxos; i++ {
			paxos[i].SetUnreliable(false)
		}

		waitDecidedPeer(t, paxos, seq, 5)
	}

	fmt.Println("  Passed")
}

// Flaky Network

// Test multiple proposers under unreliable network
// all replicas should reach to consistent state eventually
func TestFlakyNetworkMultipleProposers(t *testing.T) {
	fmt.Println("Test: Multiple Proposers in same slots, unreliable network")
	numPaxos := 3

	paxos, _ := randPaxos(numPaxos)
	for _, px := range paxos {
		px.SetUnreliable(true)
	}

	defer ShutDown(paxos)

	msgs := make([][]string, numPaxos)

	numSlot := 30

	for i := range msgs {
		msgs[i] = randValues(numSlot)
	}

	for i := 0; i < numSlot; i++ {
		for j := 0; j < numPaxos; j++ {
			paxos[j].Start(i, msgs[j][i])
		}
		time.Sleep(30 * time.Millisecond)
	}

	for _, px := range paxos {
		px.SetUnreliable(false)
	}

	for i := 0; i < numSlot; i++ {
		waitDecidedPeer(t, paxos, i, numPaxos)
		_, val := paxos[0].Status(i)
		flag := false

		for j := 0; j < numPaxos; j++ {
			if msgs[j][i] == val {
				flag = true
				break
			}
		}

		if !flag {
			t.Fatal("Value chosen is not from a single proposer!")
		}
	}

	fmt.Println("  Passed!")
}

// Fail-Tolerance
func TestMinorityFail(t *testing.T) {
	fmt.Println("Test: Minority Fail, the rest of the replicas should still reach agreement")
	numPaxos := 5

	paxos, _ := randPaxos(numPaxos)

	defer ShutDown(paxos)

	msgs := make([][]string, numPaxos)

	numSlot := 30

	for i := range msgs {
		msgs[i] = randValues(numSlot)
	}

	paxos[numPaxos-1].Kill()

	for i := 0; i < numSlot/2; i++ {
		for j := 0; j < numPaxos/2; j++ {
			paxos[j].Start(i, msgs[j][i])
		}
		time.Sleep(20 * time.Millisecond)
	}

	paxos[numPaxos-2].Kill()

	for i := numSlot / 2; i < numSlot; i++ {
		for j := 0; j < numPaxos/2; j++ {
			paxos[j].Start(i, msgs[j][i])
		}
		time.Sleep(30 * time.Millisecond)
	}

	for i := 0; i < numSlot; i++ {
		waitDecidedPeer(t, paxos, i, numPaxos-2)
		_, val := paxos[0].Status(i)
		flag := false

		for j := 0; j < numPaxos-2; j++ {
			if msgs[j][i] == val {
				flag = true
				break
			}
		}

		if !flag {
			t.Fatal("Value chosen is not from a single proposer!")
		}
	}

	fmt.Println("  Passed!")
}

// Fail-Tolerance
func TestMajorityFail(t *testing.T) {
	fmt.Println("Test: Majority Fail, the agreement should never be reached")
	numPaxos := 5

	paxos, _ := randPaxos(numPaxos)

	defer ShutDown(paxos)

	msgs := make([][]string, numPaxos)

	numSlot := 30

	for i := range msgs {
		msgs[i] = randValues(numSlot)
	}

	paxos[numPaxos-1].Kill()
	paxos[numPaxos-2].Kill()

	for i := 0; i < numSlot-1; i++ {
		for j := 0; j < 2; j++ {
			paxos[j].Start(i, msgs[j][i])
		}
		time.Sleep(30 * time.Millisecond)
	}

	for i := 0; i < numSlot-1; i++ {
		waitDecidedPeer(t, paxos, i, numPaxos-2)
		_, val := paxos[0].Status(i)
		flag := false

		for j := 0; j < 2; j++ {
			if msgs[j][i] == val {
				flag = true
				break
			}
		}

		if !flag {
			t.Fatal("Value chosen is not from a single proposer!", val)

		}
	}

	paxos[numPaxos-3].Kill()

	for i := numSlot - 1; i < numSlot; i++ {
		for j := 0; j < numPaxos/2; j++ {
			paxos[j].Start(i, msgs[j][i])
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !timeoutWait(t, paxos[0:numPaxos-3], numSlot-1) {
		t.Fatal("Reach agreement when majority failed")
	}
	fmt.Println("  Passed!")
}

func TestCatchUp(t *testing.T) {
	fmt.Println("Test: Catch Up. The replica that lags behind will only learn old value")
	paxos, _ := randPaxos(5)
	defer ShutDown(paxos)

	msgs := randValues(10)
	paxos[4].SetDeaf(true)
	for i, v := range msgs {
		paxos[0].Start(i, v)
		waitDecidedPeer(t, paxos, i, 4)
		decided, _ := paxos[4].Status(i)
		if decided {
			t.Fatal("Deaf peer heard about decision")
		}
	}

	paxos[4].SetDeaf(false)
	for i := range msgs {
		paxos[4].Start(i, 0)
		_, v := paxos[4].Status(i)
		if v == 0 {
			t.Fatal("Peer that lags behind does not learn old value")
		}
	}

	fmt.Println("  Passed!")
}

/********************
// Benchmarks
********************/

// Perfect network
func BenchmarkSingleProposer(b *testing.B) {
	paxos, _ := randPaxos(5)
	defer ShutDown(paxos)

	msgs := randValues(500)
	for i, v := range msgs {
		paxos[0].Start(i, v)
		waitDecidedPeer(b, paxos, i, 5)
	}

	for i, v := range msgs {
		ok, vv := paxos[0].Status(i)
		if !ok || vv.(string) != v {
			b.Fatal("Value not match!", v, " ", vv)
			return
		}
	}
}
