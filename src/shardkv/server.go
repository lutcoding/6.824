package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type State int

const (
	notOwn State = iota
	serving
	waiting
	sending
	erasing
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	Op         string // "Put" or "Append" or "UpdateConfig"
	ClerkId    int64
	Sequence   int
	Config     shardctrler.Config
	KVShard    map[int]*Shard
	ConfigNum  int
	EraseShard []int
}
type requestInfo struct {
	term  int
	err   Err
	value string
}
type Shard struct {
	KV        map[string]string
	Duplicate map[int64]int
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck          *shardctrler.Clerk
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions 5.
	pushCond    *sync.Cond
	eraseCond   *sync.Cond
	config      shardctrler.Config
	lastApplied int
	data        [shardctrler.NShards]*Shard
	channel     map[int]chan *requestInfo
	shardState  [shardctrler.NShards]State
	sendTo      [shardctrler.NShards]int
}

func (kv *ShardKV) executeGetCommand(op *Op, info *requestInfo) {
	err := kv.checkShard(op.Key)
	if err != OK {
		info.err = err
		return
	}
	shard := key2shard(op.Key)
	value, ok := kv.data[shard].KV[op.Key]
	if !ok {
		info.value = ""
		info.err = ErrNoKey
	} else {
		info.value = value
		info.err = OK
	}
}
func (kv *ShardKV) executePutAppendCommand(op *Op, info *requestInfo) {
	err := kv.checkShard(op.Key)
	if err != OK {
		info.err = err
		return
	}
	shard := key2shard(op.Key)
	if i, ok := kv.data[shard].Duplicate[op.ClerkId]; ok && i >= op.Sequence {
		return
	}
	kv.data[shard].Duplicate[op.ClerkId] = op.Sequence
	if op.Op == "Put" {
		kv.data[shard].KV[op.Key] = op.Value
	} else if op.Op == "Append" {
		kv.data[shard].KV[op.Key] += op.Value
	}
	//fmt.Printf("gid: %v server:%v put request key: %v success\n", kv.gid, kv.me, op.Key)
}
func (kv *ShardKV) executeUpdateConfig(op *Op) {
	if op.Config.Num <= kv.config.Num {
		return
	}
	latest := op.Config
	outdate := kv.config
	gid := kv.gid
	if latest.Num == 1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if latest.Shards[i] == gid {
				kv.shardState[i] = serving
			}
		}
		kv.config = latest
		return
	}
	for i := 0; i < shardctrler.NShards; i++ {
		if outdate.Shards[i] == gid && latest.Shards[i] != gid {
			kv.shardState[i] = sending
			kv.sendTo[i] = latest.Shards[i]
		}
		if latest.Shards[i] == gid && outdate.Shards[i] != gid {
			kv.shardState[i] = waiting
		}
	}
	kv.config = latest
	/*	if _, isLeader := kv.rf.GetState(); isLeader {
		fmt.Printf("time: %v gid: %v server: %v update config %v %v %v latest: %v\n", time.Now().Format("15:04:05"), kv.gid, kv.me, kv.shardState, outdate.Num, latest.Num, gids)
	}*/
}
func (kv *ShardKV) executePush(op *Op, info *requestInfo) {
	if kv.config.Num > op.ConfigNum {
		return
	} else if op.ConfigNum > kv.config.Num {
		info.err = ErrConfigNotReady
		return
	}
	/*	if _, isLeader := kv.rf.GetState(); isLeader {
		shard := []int{}
		for i, _ := range op.KVShard {
			shard = append(shard, i)
		}
		fmt.Printf("time: %v gid: %v server: %v exute push: %v\n", time.Now().Format("15:04:05"), kv.gid, kv.me, shard)
	}*/
	for i, shard := range op.KVShard {
		if kv.shardState[i] == waiting {
			kv.shardState[i] = serving
			for key, value := range shard.KV {
				kv.data[i].KV[key] = value
			}
			for clerk, sequence := range shard.Duplicate {
				kv.data[i].Duplicate[clerk] = sequence
			}
		}
	}
	//fmt.Printf("gid: %v server: %v executePush %v\n", kv.gid, kv.me, kv.shardState)
}
func (kv *ShardKV) executeErase(op *Op) {
	//fmt.Printf("gid: %v server: %v executeErase %v\n", kv.gid, kv.me, op.EraseShard)
	if op.ConfigNum != kv.config.Num {
		return
	}
	for _, i := range op.EraseShard {
		if kv.shardState[i] != notOwn {
			kv.sendTo[i] = 0
			kv.shardState[i] = notOwn
			kv.data[i] = &Shard{map[string]string{}, map[int64]int{}}
		}
	}
}
func (kv *ShardKV) checkPush() bool {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardState[i] == sending {
			return true
		}
	}
	return false
}
func (kv *ShardKV) checkErase() bool {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardState[i] == erasing {
			return true
		}
	}
	return false
}
func (kv *ShardKV) push() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * 50)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && kv.checkPush() {
			//gid -> shards
			//gid: 要push给的组
			//shards: 要发过去的全部shard
			pushMsg := map[int][]int{}
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.shardState[i] == sending {
					pushMsg[kv.sendTo[i]] = append(pushMsg[kv.sendTo[i]], i)
				}
			}
			for gid, shards := range pushMsg {
				if servers, ok := kv.config.Groups[gid]; ok {
					//fmt.Printf("time: %v gid: %v server: %v send shard: %v to gid: %v\n", time.Now().Format("15:04:05"), kv.gid, kv.me, shards, gid)
					args := PushArgs{map[int]*Shard{}, kv.config.Num}
					for _, i := range shards {
						args.KVShard[i] = &Shard{map[string]string{}, map[int64]int{}}
						for key, value := range kv.data[i].KV {
							args.KVShard[i].KV[key] = value
						}
						for clerk, sequence := range kv.data[i].Duplicate {
							args.KVShard[i].Duplicate[clerk] = sequence
						}
					}
					go func(args *PushArgs, servers []string, make_end func(string) *labrpc.ClientEnd, kv *ShardKV, gid int) {
						for si := 0; si < len(servers); si++ {
							srv := make_end(servers[si])
							var reply PutAppendReply
							ok := srv.Call("ShardKV.Push", args, &reply)
							if ok && reply.Err == OK {
								kv.mu.Lock()
								for i, _ := range args.KVShard {
									if kv.shardState[i] == sending {
										kv.shardState[i] = erasing
									}
									if kv.sendTo[i] != 0 {
										kv.sendTo[i] = 0
									}
								}
								//fmt.Printf("gid: %v server: %v push ok %v\n", kv.gid, kv.me, kv.shardState)
								kv.mu.Unlock()

								break
							}
							if ok && reply.Err == ErrConfigNotReady {
								//fmt.Printf("gid: %v server: %v send to gid: %v failed\n", kv.gid, kv.me, gid)
								time.Sleep(time.Millisecond * 50)
								continue
							}
						}
					}(&args, servers, kv.make_end, kv, gid)
				}
			}
		}
		kv.mu.Unlock()
	}
}
func (kv *ShardKV) erase() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * 50)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader && kv.checkErase() {
			eraseShard := []int{}
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.shardState[i] == erasing {
					eraseShard = append(eraseShard, i)
				}
			}
			op := Op{EraseShard: eraseShard, Op: "Erase", ConfigNum: kv.config.Num}
			kv.eraseCond.L.Unlock()
			kv.rf.Start(op)
		} else {
			kv.mu.Unlock()
		}
	}
}
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			if kv.lastApplied >= applyMsg.CommandIndex {
				kv.mu.Unlock()
				continue
			}
			info := &requestInfo{err: OK, term: applyMsg.Term}
			kv.lastApplied = applyMsg.CommandIndex
			if op.Op == "Put" || op.Op == "Append" {
				kv.executePutAppendCommand(&op, info)
			} else if op.Op == "Get" {
				kv.executeGetCommand(&op, info)
			} else if op.Op == "Push" {
				kv.executePush(&op, info)
			} else if op.Op == "Erase" {
				kv.executeErase(&op)
			} else if op.Op == "UpdateConfig" {
				kv.executeUpdateConfig(&op)
			}
			kv.mu.Unlock()
			if term, isLeader := kv.rf.GetState(); isLeader && term == applyMsg.Term &&
				op.Op != "UpdateConfig" && op.Op != "Erase" && op.Op != "Empty" {
				channel := kv.getChan(applyMsg.CommandIndex)
				channel <- info
			}
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			if applyMsg.SnapshotIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			//fmt.Printf("gid: %v server: %v execute snapshot\n", kv.gid, kv.me)
			kv.lastApplied = applyMsg.SnapshotIndex
			kv.readSnapShot(applyMsg.Snapshot)
			kv.mu.Unlock()
		}
	}
}
func (kv *ShardKV) fetchConfig() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		isReady := true
		kv.mu.Lock()
		currentConfigNum := kv.config.Num
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardState[i] != notOwn && kv.shardState[i] != serving {
				isReady = false
				break
			}
		}
		kv.mu.Unlock()
		if isReady {
			config := kv.mck.Query(currentConfigNum + 1)
			if config.Num == currentConfigNum+1 {
				op := Op{Op: "UpdateConfig", Config: config}
				kv.rf.Start(op)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}
func (kv *ShardKV) checkLastLogTerm() {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * 100)
		if term, isLeader := kv.rf.GetState(); isLeader && kv.rf.CheckLastLogTerm() < term {
			op := Op{Op: "Empty"}
			kv.rf.Start(op)
		}
	}
}
func (kv *ShardKV) checkShard(key string) Err {
	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	if gid != kv.gid {
		return ErrWrongGroup
	} else if kv.shardState[shard] == waiting {
		return ErrConfigNotReady
	}
	return OK
}
func (kv *ShardKV) getChan(index int) chan *requestInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.channel[index]; !ok {
		kv.channel[index] = make(chan *requestInfo)
	}
	return kv.channel[index]
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	/*	kv.mu.Lock()
		reply.Err = kv.checkShard(args.Key)
		kv.mu.Unlock()
		if reply.Err != OK {
			return
		}*/
	op := Op{Key: args.Key, Op: "Get", ClerkId: args.ClerkId, Sequence: args.Sequence}
	index, term, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	channel := kv.getChan(index)
	select {
	case r := <-channel:
		if r.term != term {
			return
		}
		reply.Err, reply.Value = r.err, r.value
	case <-time.After(time.Second):
		return
	}
	//fmt.Printf("gid: %v server: %v execute get key: %v value: %v\n", kv.gid, kv.me, args.Key, reply.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	/*	kv.mu.Lock()
		reply.Err = kv.checkShard(args.Key)
		kv.mu.Unlock()
		if reply.Err != OK {
			return
		}*/
	op := Op{
		Key: args.Key, Value: args.Value, Op: args.Op, ClerkId: args.ClerkId, Sequence: args.Sequence,
	}
	index, term, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	channel := kv.getChan(index)
	select {
	case r := <-channel:
		if r.term != term {
			return
		}
		reply.Err = r.err
	case <-time.After(time.Second):
		return
	}
	//fmt.Printf("gid: %v server:%v put request key: %v success\n", kv.gid, kv.me, args.Key)
}
func (kv *ShardKV) Push(args *PushArgs, reply *PushReply) {
	op := Op{KVShard: args.KVShard, ConfigNum: args.ConfigNum, Op: "Push"}
	index, term, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	channel := kv.getChan(index)
	select {
	case r := <-channel:
		if r.term != term {
			return
		}
		reply.Err = r.err
	case <-time.After(time.Second):

		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *ShardKV) snapshot(persister *raft.Persister) {
	for kv.killed() == false {
		time.Sleep(time.Millisecond * 20)
		if len(persister.ReadRaftState()) >= int(float64(kv.maxraftstate)/1.5) {
			kv.persistSnapShot()
		}
	}
}
func (kv *ShardKV) persistSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.data)
	e.Encode(kv.lastApplied)
	e.Encode(kv.config)
	e.Encode(kv.shardState)
	e.Encode(kv.sendTo)
	data := w.Bytes()
	index := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.Snapshot(index, data)
}
func (kv *ShardKV) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	tmpKV := [shardctrler.NShards]*Shard{}
	var index int
	var c shardctrler.Config
	var shardState [shardctrler.NShards]State
	var sendTo [shardctrler.NShards]int
	if d.Decode(&tmpKV) != nil || d.Decode(&index) != nil || d.Decode(&c) != nil ||
		d.Decode(&shardState) != nil || d.Decode(&sendTo) != nil {
		fmt.Printf("readPersist fail")
	} else {
		kv.data = tmpKV
		kv.lastApplied = index
		kv.config = c
		kv.shardState = shardState
		kv.sendTo = sendTo
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = 0
	kv.channel = make(map[int]chan *requestInfo)
	kv.config = shardctrler.Config{Num: 0}
	kv.pushCond = sync.NewCond(&kv.mu)
	kv.eraseCond = sync.NewCond(&kv.mu)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = &Shard{map[string]string{}, map[int64]int{}}
		kv.shardState[i] = notOwn
	}
	kv.readSnapShot(persister.ReadSnapshot())
	go kv.applier()
	if maxraftstate != -1 {
		go kv.snapshot(persister)
	}
	go kv.fetchConfig()
	go kv.push()
	go kv.erase()
	go kv.checkLastLogTerm()

	return kv
}
