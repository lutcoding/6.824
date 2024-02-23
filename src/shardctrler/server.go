package shardctrler

import (
	"6.824/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type op string

const (
	join  op = "Join"
	leave op = "Leave"
	move  op = "Move"
	query op = "Query"
)

type gid struct {
	gids     []int
	shardNum map[int]int
}

func (g gid) Less(i, j int) bool {
	a, b := g.gids[i], g.gids[j]
	if g.shardNum[a] != g.shardNum[b] {
		return g.shardNum[a] > g.shardNum[b]
	} else {
		return a < b
	}
}
func (g gid) Len() int {
	return len(g.gids)
}
func (g gid) Swap(i, j int) {
	tmp := g.gids[i]
	g.gids[i] = g.gids[j]
	g.gids[j] = tmp
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	// Your data here.
	shardNum     map[int]int
	shardState   [NShards]bool
	configs      []Config // indexed by config num
	configNumber int
	duplicate    map[int64]int
	channel      map[int]chan int
}

func (sc *ShardCtrler) getChan(index int) chan int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.channel[index]; !ok {
		sc.channel[index] = make(chan int)
	}
	return sc.channel[index]
}
func (sc *ShardCtrler) addConfig() {
	sc.configNumber++
	tmp := sc.configNumber
	sc.configs = append(sc.configs, Config{
		Num: tmp, Shards: sc.configs[tmp-1].Shards,
	})
	sc.configs[tmp].Groups = make(map[int][]string)
	for i, strings := range sc.configs[tmp-1].Groups {
		sc.configs[tmp].Groups[i] = strings
	}
}
func (sc *ShardCtrler) isDuplicate(clerkId int64, sequence int) bool {
	if i, ok := sc.duplicate[clerkId]; ok && i >= sequence {
		return true
	}
	sc.duplicate[clerkId] = sequence
	return false
}
func sortGid(sortNum map[int]int) []int {
	gid := gid{gids: []int{}, shardNum: sortNum}
	for i, _ := range sortNum {
		gid.gids = append(gid.gids, i)
	}
	sort.Sort(gid)
	return gid.gids
}
func (sc *ShardCtrler) reShard() {
	length := len(sc.shardNum)
	if length == 0 {
		return
	}
	avgSize := NShards / length
	superfluous := NShards % length
	gids := sortGid(sc.shardNum)
	for i := 0; i < length; i++ {
		target := adjustSize(avgSize, superfluous, i)
		if sc.shardNum[gids[i]] <= target {
			continue
		}
		for j, k := 0, sc.shardNum[gids[i]]-target; j < NShards && 0 < k; j++ {
			if sc.configs[sc.configNumber].Shards[j] == gids[i] {
				sc.shardState[j] = false
				k--
			}
		}
		sc.shardNum[gids[i]] = target
	}

	for i := 0; i < length; i++ {
		target := adjustSize(avgSize, superfluous, i)
		if sc.shardNum[gids[i]] >= target {
			continue
		}
		for j, k := 0, target-sc.shardNum[gids[i]]; j < NShards && 0 < k; j++ {
			if !sc.shardState[j] {
				sc.shardState[j] = true
				sc.configs[sc.configNumber].Shards[j] = gids[i]
				k--
			}
		}
		sc.shardNum[gids[i]] = target
	}
}
func adjustSize(avgSize, superfluous, index int) int {
	if index < superfluous {
		return avgSize + 1
	} else {
		return avgSize
	}
}

func (sc *ShardCtrler) executeJoin(op *Op) {
	if sc.isDuplicate(op.ClerkId, op.Sequence) {
		return
	}
	sc.addConfig()
	for i, strings := range op.Servers {
		if _, ok := sc.configs[sc.configNumber].Groups[i]; !ok {
			sc.configs[sc.configNumber].Groups[i] = strings
			sc.shardNum[i] = 0
		}
	}
	sc.reShard()
}
func (sc *ShardCtrler) executeLeave(op *Op) {
	if sc.isDuplicate(op.ClerkId, op.Sequence) {
		return
	}
	sc.addConfig()
	for _, gid := range op.Gids {
		delete(sc.configs[sc.configNumber].Groups, gid)
		delete(sc.shardNum, gid)
		for i := 0; i < NShards; i++ {
			if sc.configs[sc.configNumber].Shards[i] == gid {
				sc.shardState[i] = false
			}
		}
	}
	sc.reShard()
}
func (sc *ShardCtrler) executeMove(op *Op) {
	if sc.isDuplicate(op.ClerkId, op.Sequence) {
		return
	}
	sc.addConfig()
	sc.shardNum[sc.configs[sc.configNumber].Shards[op.Shard]]--
	sc.shardNum[op.Gid]++
	sc.configs[sc.configNumber].Shards[op.Shard] = op.Gid
}
func (sc *ShardCtrler) execute(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if op.Op == join {
		sc.executeJoin(&op)
	} else if op.Op == leave {
		sc.executeLeave(&op)
	} else if op.Op == move {
		sc.executeMove(&op)
	}
}
func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		applyAsg := <-sc.applyCh
		if term, isLeader := sc.rf.GetState(); isLeader && term == applyAsg.Term {
			channel := sc.getChan(applyAsg.CommandIndex)
			channel <- term
		}
		sc.execute(applyAsg.Command.(Op))
	}
}

type Op struct {
	// Your data here.
	Op       op
	Shard    int
	Gid      int
	Gids     []int
	Servers  map[int][]string
	ClerkId  int64
	Sequence int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = true
	op := Op{Op: join, ClerkId: args.ClerkId, Sequence: args.Sequence, Servers: args.Servers}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	channel := sc.getChan(index)
	select {
	case tmp := <-channel:
		if tmp == term {
			reply.WrongLeader = false
		}
	case <-time.After(time.Second):
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = true
	op := Op{Op: leave, ClerkId: args.ClerkId, Sequence: args.Sequence, Gids: args.GIDs}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	channel := sc.getChan(index)
	select {
	case tmp := <-channel:
		if tmp == term {
			reply.WrongLeader = false
		}
	case <-time.After(time.Second):
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	op := Op{Op: move, ClerkId: args.ClerkId,
		Sequence: args.Sequence, Shard: args.Shard, Gid: args.GID}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	channel := sc.getChan(index)
	select {
	case tmp := <-channel:
		if tmp == term {
			reply.WrongLeader = false
		}
	case <-time.After(time.Second):
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = true
	op := Op{Op: query, ClerkId: args.ClerkId, Sequence: args.Sequence}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}
	channel := sc.getChan(index)
	select {
	case tmp := <-channel:
		if tmp != term {
			return
		}
	case <-time.After(time.Second):
		return
	}
	reply.WrongLeader = false
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if args.Num == -1 || args.Num > sc.configNumber {
		reply.Config = sc.configs[sc.configNumber]
		//fmt.Printf("server: %v now shard: %v\n", sc.me, reply.Config.Shards)
	} else {
		reply.Config = sc.configs[args.Num]
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.configNumber = 0
	sc.duplicate = make(map[int64]int)
	sc.channel = make(map[int]chan int)
	sc.shardNum = make(map[int]int)

	// Your code here.
	go sc.applier()

	return sc
}
