package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (op Op) equal(a *Op) bool {
	if op.Op == a.Op && op.Key == a.Key && op.Value == a.Value {
		return true
	}
	return false
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClerkId  int64
	Sequence int
}
type requestInfo struct {
	term int
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int
	kv          map[string]string
	duplicate   map[int64]int
	channel     map[int]chan *requestInfo
}

func (kv *KVServer) getChan(index int) chan *requestInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.channel[index]; !ok {
		kv.channel[index] = make(chan *requestInfo)
	}
	return kv.channel[index]
}
func (kv *KVServer) executePutAppendCommand(op *Op) {
	if i, ok := kv.duplicate[op.ClerkId]; ok && i >= op.Sequence {
		return
	}
	kv.duplicate[op.ClerkId] = op.Sequence
	if op.Op == "Put" {
		kv.kv[op.Key] = op.Value
	} else if op.Op == "Append" {
		kv.kv[op.Key] += op.Value
	}
}
func (kv *KVServer) executeGetCommand(key string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.kv[key]; !ok {
		return "", ErrNoKey
	}
	return kv.kv[key], OK
}
func (kv *KVServer) executeSnapshot() {

}
func (kv *KVServer) applier() {
	for kv.killed() == false {
		applyAsg := <-kv.applyCh
		//当前term的applyAsg，需要unblock channel
		if applyAsg.CommandValid {
			if term, isLeader := kv.rf.GetState(); isLeader && term == applyAsg.Term {
				channel := kv.getChan(applyAsg.CommandIndex)
				channel <- &requestInfo{applyAsg.Term}
			}
			op := applyAsg.Command.(Op)
			kv.mu.Lock()
/*			if applyAsg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}*/
			kv.lastApplied = applyAsg.CommandIndex
			if op.Op != "Get" {
				kv.executePutAppendCommand(&op)
			}
			kv.mu.Unlock()
		} else if applyAsg.SnapshotValid {
			kv.mu.Lock()
/*			if applyAsg.SnapshotIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}*/
			kv.lastApplied = applyAsg.SnapshotIndex
			kv.readSnapShot(applyAsg.Snapshot)
			kv.mu.Unlock()
		}
	}
}
func (kv *KVServer) deleteIndex(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.channel[index])
	delete(kv.channel, index)
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	op := Op{
		args.Key, "", "Get", args.ClerkId, args.Sequence,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Printf("leader: %v get index: %v\n", kv.me, index)
	channel := kv.getChan(index)
	select {
	case info := <-channel:
		if info.term != term {
			reply.Err = ErrLostLeadership
			return
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
		return
	}
	/*	go func() {
		kv.deleteIndex(index)
	}()*/
	reply.Value, reply.Err = kv.executeGetCommand(op.Key)
	//fmt.Printf("execute get clerkId: %v , sequence: %v\n", args.ClerkId, args.Sequence)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK
	op := Op{
		args.Key, args.Value, args.Op, args.ClerkId, args.Sequence,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Printf("leader: %v putappend index: %v\n", kv.me, index)
	channel := kv.getChan(index)
	select {
	case info := <-channel:
		if info.term != term {
			reply.Err = ErrLostLeadership
			return
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
		return
	} /*
		go func() {
			kv.deleteIndex(index)
		}()*/
	//fmt.Printf("execute putappend clerkId: %v , sequence: %v\n", args.ClerkId, args.Sequence)
}
func (kv *KVServer) snapshot(persister *raft.Persister) {
	for {
		time.Sleep(time.Millisecond * 20)
		//multiple := 0.5
		if len(persister.ReadRaftState()) >= int(float64(kv.maxraftstate)/1.5) {
			kv.persistSnapShot()
		}
	}
}
func (kv *KVServer) persistSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.kv)
	e.Encode(kv.duplicate)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	index := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.Snapshot(index, data)
}
func (kv *KVServer) readSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	tmpKV, tmpDuplicate := map[string]string{}, map[int64]int{}
	var index int
	if d.Decode(&tmpKV) != nil || d.Decode(&tmpDuplicate) != nil || d.Decode(&index) != nil {
		fmt.Printf("readPersist fail")
	} else {
		kv.kv = tmpKV
		kv.duplicate = tmpDuplicate
		kv.lastApplied = index
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kv = make(map[string]string)
	// You may need initialization code here.
	kv.duplicate = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.lastApplied = 0
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.channel = map[int]chan *requestInfo{}
	kv.readSnapShot(persister.ReadSnapshot())
	// You may need initialization code here.

	go kv.applier()
	if maxraftstate != -1 {
		go kv.snapshot(persister)
	}
	return kv
}
