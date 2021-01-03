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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

type State string
const(
	Leader State = "Leader"
	Candidate = "Candidate"
	Follower = "Follower"
)

const NotVoted = -1
// election time out. Paper uses 150-300ms, for this assignment we try a larger number
const ElectionTimeOutMin = 600
const ElectionTimeOutMax = 1000
// a small amount added to avoid precision error in timer.sleep, which might cause an
// extra time out interval to be waited.
const TimeOutResetEps = 10
// time elapsed between each AppendEntry
const AppendEntryInterval = 200

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	latestReset time.Time         // time of latest reset, used for timeouts
	state     State               // State of the Raft peer, one of leader, candidate, follower
	currentTerm      int          // currentTerm latest term server has seen (initialized to 0 on first boot, increases monotonically)
    votedFor  int                 // candidateId that received vote in current term (or nil if none)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%v received vote request from %v", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("Vote not granted from %v to %v", rf.me, args.CandidateId)
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("Vote granted from %v to %v", rf.me, args.CandidateId)
	} else if args.Term == rf.currentTerm && (rf.votedFor == NotVoted || rf.votedFor == args.CandidateId){
		reply.VoteGranted = true
		DPrintf("Vote granted from %v to %v", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = false
		DPrintf("Vote not granted from %v to %v", rf.me, args.CandidateId)
	}
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


//
// AppendEntry RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
}

//
// AppendEntry RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v received AppendEntry from %v", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if rf.state == Leader {
		rf.state = Follower
		go rf.operateFollower()
	} else if rf.state == Candidate {
		rf.state = Follower
	}
	DPrintf("%v processed AppendEntry from %v", rf.me, args.LeaderId)
	rf.latestReset = time.Now()
}


//
// code to send an AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

//
// get a random election timeout duration in [ElectionTimeOutMin, ElectionTimeOutMax)
//
func randElectionWaitTime() time.Duration {
	return time.Duration(rand.Int63n(ElectionTimeOutMax - ElectionTimeOutMin)) * time.Millisecond + ElectionTimeOutMin * time.Millisecond
}


//
// goroutine for followers that kicks off an election when timer goes
// off. reset are implemented with rf.latestReset.
//
func (rf *Raft) operateFollower() {
	timeToWait := randElectionWaitTime()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() && rf.state == Follower {
		// wait at the beginning of the function since we want a timeout
		// at initial start up.
		latestReset := rf.latestReset
		rf.mu.Unlock()
		time.Sleep(timeToWait - time.Since(latestReset) + TimeOutResetEps)
		rf.mu.Lock()
		// timed out. Start election.
		if rf.state == Follower && time.Since(rf.latestReset) > timeToWait {
			rf.state = Candidate
			for rf.state == Candidate {
				DPrintf("%v starts election at term %v", rf.me, rf.currentTerm)
				rf.kickOffElection()
				if rf.state != Candidate {
					break
				}
				rf.mu.Unlock()
				time.Sleep(randElectionWaitTime())
				rf.mu.Lock()
			}
		}
		timeToWait = randElectionWaitTime()
	}
}

var callNo = 0
//
// goroutine for leaders. Send AppendEntries periodically.
//
func (rf *Raft) operateLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() && rf.state == Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		for i:=0; i<len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := AppendEntriesReply{}
				thisCallNo := callNo
				callNo ++
				DPrintf("%v sends AppendEntry to %v [%v]", rf.me, i, thisCallNo)
				ok := rf.sendAppendEntries(i, &AppendEntriesArgs{term, rf.me}, &reply)
				DPrintf("%v sent AppendEntry to %v, status %v [%v]", rf.me, i, ok, thisCallNo)
				if ok {
					rf.mu.Lock()
					if rf.state == Leader && rf.currentTerm < reply.Term {
						rf.state = Follower
						go rf.operateFollower()
					}
					rf.mu.Unlock()
				}
			} (i)
		}
		time.Sleep(AppendEntryInterval * time.Millisecond)
		rf.mu.Lock()
	}
}

//
// function to kick off an election. the function enters with the lock held.
//
func  (rf *Raft) kickOffElection() {
	rf.currentTerm ++
	rf.votedFor = rf.me
	votesNeeded := len(rf.peers) / 2
	term := rf.currentTerm
	rf.mu.Unlock()
	c := make(chan bool)
	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			args := RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
			}
			DPrintf("%v requests vote from %v", rf.me, i)
			ok := rf.sendRequestVote(i, &args, &reply)
			c <- ok && reply.VoteGranted
		} (i)
	}
	votesReceived, votesProcessed := 0, 0
	done := false
	for i:=0;i<len(rf.peers)-1;i++ {
		select {
			case vote := <-c: {
				if vote {
					votesReceived++
				}
				done = votesReceived >= votesNeeded || votesProcessed == len(rf.peers) - 1
			}
			case <- time.After(randElectionWaitTime()):
				done = true
		}
		if done {
			break
		}
	}

	//for vote := range c {
	//	if vote {
	//		votesReceived++
	//	}
	//	votesProcessed++
	//	DPrintf("%v processed 1 vote response", rf.me)
	//	if votesReceived >= votesNeeded || votesProcessed == len(rf.peers) - 1 {
	//		break
	//	}
	//}
	rf.mu.Lock()
	if votesReceived >= votesNeeded && rf.state == Candidate {
		DPrintf("%v elected leader", rf.me)
		rf.state = Leader
		go rf.operateLeader()
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

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NotVoted
	rf.latestReset = time.Now()

	go rf.operateFollower()
	DPrintf("%v started", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
