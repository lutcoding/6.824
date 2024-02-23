package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/pprof"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
func StartHTTPDebuger(s string) {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: s, Handler: pprofHandler}
	go server.ListenAndServe()
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type ApplyMsg struct {
	Term         int
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type State int

const (
	Follower State = iota
	Candidate
	Leader
)
const (
	MIN       = 400
	MAX       = 500
	HeartBeat = time.Millisecond * 100
)

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

func getRand(me int) time.Duration {
	rand.Seed(time.Now().UnixNano() + int64(me))
	return time.Duration(rand.Intn(MIN)+MAX) * time.Millisecond
}
func (rf *Raft) setNewTimeOut() {
	rf.heardFromLeader = time.Now()
	rf.timeout = getRand(rf.me)
}
func (rf *Raft) CheckLastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastEnteryTerm()
}

//
func (rf *Raft) transitToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
}
func (rf *Raft) transitToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.setNewTimeOut()
}
func (rf *Raft) transitToLeader() {
	rf.state = Leader
	rf.leaderId = rf.me
	index := rf.lastEnteryIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = index + 1
		rf.matchIndex[i] = 0
	}
}

//
// A Go object implementing a single Raft peer.
//
func (rf *Raft) slice(index int) Logs {
	if index > rf.lastEnteryIndex() {
		return Logs{}
	} else {
		return rf.log[index-rf.offset:]
	}
}
func (rf *Raft) lastEnteryIndex() int {
	return rf.offset + len(rf.log) - 1
}
func (rf *Raft) lastEnteryTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshot.LastIncludedTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

type SnapShot struct {
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}
type Logs []Log
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int
	log         Logs
	offset      int //snapshot所包含的日志个数
	snapshot    *SnapShot

	timeout         time.Duration
	heardFromLeader time.Time
	leaderId        int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh           chan ApplyMsg
	applyCond         *sync.Cond
	applySnapshotCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("server: %v acquire lock in getstate\n", rf.me)
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	//fmt.Printf("server: %v lost lock in getstate\n", rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot.LastIncludedIndex)
	e.Encode(rf.snapshot.LastIncludedTerm)
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot.Data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log Logs
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("readPersist fail")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshot.LastIncludedIndex = lastIncludedIndex
		rf.snapshot.LastIncludedTerm = lastIncludedTerm
		rf.snapshot.Data = snapshot
		if lastIncludedIndex != -1 {
			rf.commitIndex = lastIncludedIndex
			rf.lastApplied = lastIncludedIndex
		}
		rf.offset = lastIncludedIndex + 1
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//fmt.Printf("receive snapshot from kv store service\n")
	//fmt.Printf("server: %v receive snapshot from kv store service\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.snapshot.LastIncludedIndex {
		lastInLog := index - rf.offset
		rf.snapshot.LastIncludedIndex = index
		rf.snapshot.LastIncludedTerm = rf.log[lastInLog].Term
		rf.snapshot.Data = snapshot
		rf.offset = index + 1
		if lastInLog == len(rf.log)-1 {
			rf.log = Logs{}
		} else {
			rf.log = rf.log[lastInLog+1:]
		}
		rf.persist()
	}

	//fmt.Printf("server: %v lost lock in snapshot\n", rf.me)
	//fmt.Printf("receive snapshot from kv store service complete\n")
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	flag := false
	if args.Term > rf.currentTerm {
		rf.transitToFollower(args.Term)
		rf.votedFor = -1
		flag = true
	}
	uptodate := args.LastLogTerm > rf.lastEnteryTerm() ||
		(args.LastLogTerm == rf.lastEnteryTerm() && args.LastLogIndex >= rf.lastEnteryIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		reply.VoteGranted = true
		rf.setNewTimeOut()
		rf.votedFor = args.CandidateId
		flag = true
	}
	if flag {
		rf.persist()
	}
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		//fmt.Printf("server: %v lost lock in InstallSnapshot\n", rf.me)
		return
	}
	if rf.snapshot.LastIncludedIndex >= args.LastIncludedIndex || rf.commitIndex >= args.LastIncludedIndex {
		//fmt.Printf("server: %v lost lock in InstallSnapshot\n", rf.me)
		return
	}
	rf.votedFor = args.LeaderId
	rf.transitToFollower(args.Term)
	rf.setNewTimeOut()
	rf.leaderId = args.LeaderId

	if rf.lastEnteryIndex() > args.LastIncludedIndex &&
		rf.log[args.LastIncludedIndex-rf.offset].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex-rf.offset+1:]
	} else {
		rf.log = Logs{}
	}
	rf.snapshot.Data = args.Data
	rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
	rf.offset = args.LastIncludedIndex + 1
	rf.persist()
	rf.commitIndex = args.LastIncludedIndex
	rf.apply()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      Logs
	LeaderCommit int
}
type Conflict struct {
	ConflictIndex int
	ConflictTerm  int
}
type AppendEntriesReply struct {
	C            Conflict
	Stale        bool
	ConflictFlag bool
	Term         int
	Succes       bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Succes = false
	reply.Stale = false
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		//fmt.Printf("server: %v lost lock in AppendEntries\n", rf.me)
		return
	}
	rf.votedFor = args.LeaderId
	rf.transitToFollower(args.Term)
	rf.setNewTimeOut()
	rf.leaderId = args.LeaderId
	if args.PreLogIndex > rf.lastEnteryIndex() {
		reply.ConflictFlag = true
		reply.C.ConflictIndex = rf.lastEnteryIndex() + 1
		reply.C.ConflictTerm = -1
		return
	}
	//follower.snapshotLastIndex <\= leader.snapshotLastIndex <= leader.commitIndex
	//follower.snapshotLastIndex > leader.snapshotLastIndex 有可能
	if args.PreLogIndex == rf.lastEnteryIndex() {
		//改进版
		if rf.snapshot.LastIncludedIndex == args.PreLogIndex ||
			rf.log[args.PreLogIndex-rf.offset].Term == args.PreLogTerm {
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
		} else {
			reply.ConflictFlag = true
			reply.C.ConflictTerm = rf.log[args.PreLogIndex-rf.offset].Term
			var firstIndex int
			for firstIndex = args.PreLogIndex; firstIndex > rf.offset; firstIndex-- {
				if rf.log[firstIndex-1-rf.offset].Term != rf.log[args.PreLogIndex-rf.offset].Term {
					break
				}
			}
			reply.C.ConflictIndex = firstIndex
			return
		}
	}
	if args.PreLogIndex < rf.lastEnteryIndex() {
		if args.PreLogIndex == rf.snapshot.LastIncludedIndex {
			if len(args.Entries) != 0 {
				argIndex, rfIndex := len(args.Entries)-1, len(rf.log)-1
				argTerm, rfTerm := args.Entries[argIndex].Term, rf.log[rfIndex].Term
				isupdate := argTerm > rfTerm || (argTerm == rfTerm && argIndex >= rfIndex)
				if !isupdate {
					reply.Stale = true
					return
				}
				rf.log = append(Logs{}, args.Entries...)
				rf.persist()
			}
		} else if args.PreLogIndex > rf.snapshot.LastIncludedIndex {
			index := args.PreLogIndex - rf.offset
			if !(index >= 0 && index < len(rf.log)) {
				reply.Stale = true
				return
			}
			if rf.log[args.PreLogIndex-rf.offset].Term == args.PreLogTerm {
				/*				rf.log = rf.log[:args.PreLogIndex+1-rf.offset]
								rf.log = append(rf.log, args.Entries...)
								rf.persist()*/

				if len(args.Entries) != 0 {
					tmpEntries := make(Logs, len(rf.log[args.PreLogIndex-rf.offset+1:]))
					copy(tmpEntries, rf.log[args.PreLogIndex-rf.offset+1:])
					argIndex, rfIndex := len(args.Entries)-1, len(tmpEntries)-1
					argTerm, rfTerm := args.Entries[argIndex].Term, tmpEntries[rfIndex].Term
					isupdate := argTerm > rfTerm || (argTerm == rfTerm && argIndex >= rfIndex)
					if !isupdate {
						reply.Stale = true
						return
					}
					for i, entry := range args.Entries {
						if rf.lastEnteryIndex() >= entry.Index && rf.log[entry.Index-rf.offset].Term != entry.Term {
							rf.log = rf.log[:entry.Index-rf.offset]
							rf.persist()
						}
						if entry.Index > rf.lastEnteryIndex() {
							rf.log = append(rf.log, args.Entries[i:]...)
							rf.persist()
							break
						}
					}
				}

			} else {
				reply.ConflictFlag = true
				reply.C.ConflictTerm = rf.log[args.PreLogIndex-rf.offset].Term
				var firstIndex int
				for firstIndex = args.PreLogIndex; firstIndex > rf.offset; firstIndex-- {
					if rf.log[firstIndex-1-rf.offset].Term != rf.log[args.PreLogIndex-rf.offset].Term {
						break
					}
				}
				reply.C.ConflictIndex = firstIndex
				fmt.Printf("")
				return
			}
		} else {
			reply.Stale = true
			return
		}
	}
	//AppendEntries RPC rules #5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastEnteryIndex())
		rf.apply()
	}
	reply.Succes = true

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
	} else {
		index = rf.lastEnteryIndex() + 1
		rf.log = append(rf.log, Log{Index: index, Term: term, Command: command})
		rf.persist()
		// 从状态机收到log马上启动一次leaderTransaction，而不是等待hearbeat时间到期
		// 有利于加快运行速度
		rf.leaderTransaction(false)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(HeartBeat)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == Leader {
			rf.leaderTransaction(true)
		} else if time.Since(rf.heardFromLeader) > rf.timeout {
			rf.electionTransaction()
		}
		rf.mu.Unlock()
	}
}

//rule Raft Structure Advice and Student's Guide
func (rf *Raft) applier() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for rf.killed() == false {
		if rf.lastApplied < rf.snapshot.LastIncludedIndex {
			rf.lastApplied = rf.snapshot.LastIncludedIndex
			msg := ApplyMsg{
				Term:          rf.currentTerm,
				SnapshotValid: true,
				CommandValid:  false,
				SnapshotTerm:  rf.snapshot.LastIncludedTerm,
				SnapshotIndex: rf.snapshot.LastIncludedIndex,
				Snapshot:      rf.snapshot.Data,
			}
			rf.applyCond.L.Unlock()
			rf.applyCh <- msg
			rf.applyCond.L.Lock()
		} else if rf.commitIndex > rf.lastApplied && rf.lastEnteryIndex() > rf.lastApplied && rf.lastApplied+1 >= rf.offset {
			rf.lastApplied++
			index := rf.lastApplied - rf.offset
			msg := ApplyMsg{
				Term:          rf.log[index].Term,
				CommandIndex:  rf.log[index].Index,
				Command:       rf.log[index].Command,
				CommandValid:  true,
				SnapshotValid: false,
			}
			rf.applyCond.L.Unlock()
			rf.applyCh <- msg
			rf.applyCond.L.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

//leader stage
func (rf *Raft) leaderTransaction(heartbeat bool) {
	for i, _ := range rf.peers {
		if i == rf.me {
			rf.setNewTimeOut()
			continue
		}
		//只有心跳和有日志要发送时才发送RPC，减少无效RPC占用网络带宽
		if rf.lastEnteryIndex() >= rf.nextIndex[i] || heartbeat {
			if rf.snapshot.LastIncludedIndex >= rf.nextIndex[i] {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshot.LastIncludedIndex,
					LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
					Data:              make([]byte, len(rf.snapshot.Data)),
				}
				copy(args.Data, rf.snapshot.Data)
				go rf.appendSnapshots(i, args)
			} else {
				preIndex := rf.nextIndex[i] - 1
				if preIndex < 0 {
					preIndex = 0
				}
				if preIndex > rf.lastEnteryIndex() {
					preIndex = rf.lastEnteryIndex()
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PreLogIndex:  preIndex,
					Entries:      make(Logs, rf.lastEnteryIndex()-preIndex),
					LeaderCommit: rf.commitIndex,
				}
				if preIndex == rf.snapshot.LastIncludedIndex {
					args.PreLogTerm = rf.snapshot.LastIncludedTerm
				} else {
					args.PreLogTerm = rf.log[preIndex-rf.offset].Term
				}
				copy(args.Entries, rf.slice(preIndex+1))
				go rf.appendEntries(i, args)
			}
		}
	}
}
func (rf *Raft) appendSnapshots(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.transitToFollower(reply.Term)
		rf.persist()
		return
	}
	if args.Term == rf.currentTerm && rf.state == Leader {
		rf.nextIndex[server] = max(args.LastIncludedIndex+1, rf.nextIndex[server])
		rf.matchIndex[server] = max(args.LastIncludedIndex, rf.matchIndex[server])
	}
}
func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{
		C: Conflict{},
	}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.transitToFollower(reply.Term)
		rf.persist()
		//rf.votedFor = -1
		return
	}
	//Leader rules #3
	if args.Term == rf.currentTerm && rf.state == Leader {
		if reply.Stale {
			return
		}
		if reply.Succes {
			match := args.PreLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[server] = max(rf.nextIndex[server], next)
			rf.matchIndex[server] = max(rf.matchIndex[server], match)
		} else if reply.ConflictFlag {
			if reply.C.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.C.ConflictIndex
			} else {
				last := rf.findLast(reply.C.ConflictTerm)
				if last != -1 {
					rf.nextIndex[server] = last + 1
				} else {
					rf.nextIndex[server] = reply.C.ConflictIndex
				}
			}
		}

		rf.checkLeaderCommit()
	}
}
func (rf *Raft) findLast(term int) int {
	for i := rf.lastEnteryIndex() - rf.offset; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i + rf.offset
		}
	}
	return -1
}

// Leader rules #4
func (rf *Raft) checkLeaderCommit() {

	// rf.offset <= rf.commitIndex + 1
	for i := rf.commitIndex + 1; i <= rf.lastEnteryIndex(); i++ {
		if rf.log[i-rf.offset].Term != rf.currentTerm {
			continue
		}
		num := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				num++
			}
			if num > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.apply()
				break
			}
		}
	}
}
func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

//election stage
func (rf *Raft) electionTransaction() {
	rf.transitToCandidate()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastEnteryIndex(),
		LastLogTerm:  rf.lastEnteryTerm(),
	}
	voteNum := 1
	var becomeLeader sync.Once
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVote(i, args, &becomeLeader, &voteNum)
	}
}
func (rf *Raft) requestVote(server int, args *RequestVoteArgs, becomeLeader *sync.Once, voteNum *int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.transitToFollower(reply.Term)
		rf.persist()
		//rf.votedFor = -1
		return
	}

	if !reply.VoteGranted || rf.currentTerm != args.Term || rf.state != Candidate {
		return
	}

	*voteNum++
	if *voteNum > len(rf.peers)/2 {
		becomeLeader.Do(func() {
			rf.transitToLeader()
			//Leader rules #1
			rf.leaderTransaction(true)
		})
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//fmt.Printf("server: %d join the cluster\n", me)
	// Your initialization code here (2A, 2B, 2C).
	rf.setNewTimeOut()
	rf.snapshot = &SnapShot{
		Data:              nil,
		LastIncludedIndex: -1,
		LastIncludedTerm:  -1,
	}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.log = Logs{}
	rf.log = append(rf.log, Log{0, 0, nil})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applySnapshotCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	//go StartHTTPDebuger(":" + strconv.Itoa(rf.me+8000))
	return rf
}
