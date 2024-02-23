package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu      sync.Mutex
	// Your data here.
	leaderId int
	clerkId  int64
	sequence int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkId = nrand()
	ck.sequence = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	ck.mu.Unlock()
	args := QueryArgs{num, ck.clerkId, ck.sequence}
	ck.sequence++
	i := ck.leaderId
	for {
		reply := QueryReply{}
		ok := ck.servers[i].Call("ShardCtrler.Query", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = i
			return reply.Config
		}
		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := JoinArgs{servers, ck.clerkId, ck.sequence}
	ck.sequence++
	i := ck.leaderId
	for {
		reply := JoinReply{}
		ok := ck.servers[i].Call("ShardCtrler.Join", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = i
			return
		}
		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := LeaveArgs{gids, ck.clerkId, ck.sequence}
	ck.sequence++
	i := ck.leaderId
	for {
		reply := LeaveReply{}
		ok := ck.servers[i].Call("ShardCtrler.Leave", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = i
			return
		}
		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := MoveArgs{shard, gid, ck.clerkId, ck.sequence}
	ck.sequence++
	i := ck.leaderId
	for {
		reply := MoveReply{}
		ok := ck.servers[i].Call("ShardCtrler.Move", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = i
			return
		}
		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
