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
	"sort"
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
const ElectionTimeOutMin = 350
const ElectionTimeOutMax = 600
// a small amount added to avoid precision error in timer.sleep, which might cause an
// extra time out interval to be waited.
const TimeOutResetEps = 10
// time elapsed between each AppendEntry
const AppendEntryInterval = 100

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
// log entries to store, basic unit of entries[] that leader sends to
// followers.
//
type Entry struct {
	Command      interface{}
	CommandIndex int
	CommandTerm  int
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
	state       State             // State of the Raft peer, one of leader, candidate, follower
	currentTerm int               // currentTerm latest term server has seen (initialized to 0 on first boot, increases monotonically)
    votedFor    int               // candidateId that received vote in current term (or nil if none)

	applyCh     chan ApplyMsg     // channel for applying committed messages
	log[]       Entry             // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	// Volatile state on all servers:
    commitIndex int               // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int               // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Volatile state on leaders: (Reinitialized after election)
	nextIndex[] int               // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex[] int              // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
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
	Term int           // candidate’s term
	CandidateId int    // candidate requesting vote
	LastLogIndex int   // index of candidate’s last log entry (§5.4)
	LastLogTerm int    // term of candidate’s last log entry (§5.4)
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
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("Vote not granted from %v to %v", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = NotVoted
		if rf.state == Leader {
			rf.state = Follower
			go rf.operateFollower()
		} else if rf.state == Candidate {
			rf.state = Follower
		}
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term == rf.currentTerm &&
		(rf.votedFor == NotVoted || rf.votedFor == args.CandidateId) &&
		(len(rf.log) == 0 || args.LastLogTerm > rf.log[len(rf.log)-1].CommandTerm ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].CommandTerm &&
				args.LastLogIndex >= rf.log[len(rf.log)-1].CommandIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.latestReset = time.Now()
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
// reason for rejecting an AppendEntry. Used for optimization
//
type RejectAppendReason string
const (
	ByTerm RejectAppendReason = "ByTerm"
	ByConflict = "ByConflict"
	ByLength = "ByLength"
)

//
// AppendEntry RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term          int    // leader’s term
	LeaderId      int    // so follower can redirect clients
	PrevLogIndex  int    // index of log entry immediately preceding new ones
	PrevLogTerm   int    // term of prevLogIndex entry
	Entries[]     Entry  // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit  int    // leader’s commitIndex
}

//
// AppendEntry RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool
	// optimizations for fast backup.
	RejectReason RejectAppendReason
	RejectionIndex int
	RejectionTerm int
	RejectionLength int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v received AppendEntry from %v", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.RejectReason = ByTerm
		DPrintf("%v rejected AppendEntry from %v", rf.me, args.LeaderId)
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex != 0 && (
		args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].CommandTerm != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PrevLogIndex > len(rf.log) {
			reply.RejectReason = ByLength
			reply.RejectionLength = len(rf.log)
		} else {
			reply.RejectReason = ByConflict
			reply.RejectionTerm = rf.log[args.PrevLogIndex-1].CommandTerm
			reply.RejectionIndex = sort.Search(args.PrevLogIndex, func(i int) bool {
				return rf.log[i].CommandTerm == rf.log[args.PrevLogIndex-1].CommandTerm
			}) + 1
		}
		DPrintf("%v processed unmatched AppendEntry from %v", rf.me, args.LeaderId)
		rf.latestReset = time.Now()
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the
	//    existing entry and all that follow it (§5.3)
	for j:=0; j<len(args.Entries); j++ {
		if len(rf.log) > args.PrevLogIndex + j && rf.log[args.PrevLogIndex + j].CommandTerm != args.Entries[j].CommandTerm {
			rf.log = rf.log[:args.PrevLogIndex + j]
			break
		}
	}
	// 4. Append any new entries not already in the log
	for j, entry := range args.Entries {
		if len(rf.log) <= args.PrevLogIndex + j {
			rf.log = append(rf.log, entry)
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := args.PrevLogIndex + len(args.Entries)
		if lastEntryIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIndex
		}
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = true
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, Entry{
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
		})
		rf.nextIndex[rf.me] ++
		rf.matchIndex[rf.me] ++
		DPrintf("started: %v", index)
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
	rf.latestReset = time.Now()
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
				//if rf.state != Candidate {
				//	break
				//}
				//rf.mu.Unlock()
				//time.Sleep(randElectionWaitTime())
				//rf.mu.Lock()
			}
		}
		timeToWait = randElectionWaitTime()
	}
}

//
// goroutine for leaders. Send AppendEntries periodically.
//
func (rf *Raft) operateLeader() {
	// time.Sleep(AppendEntryInterval * time.Millisecond)
	rf.mu.Lock()
	// leader Initialization
	for i:=0; i<len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log)
		}
	}
	term := rf.currentTerm
	defer rf.mu.Unlock()
	for !rf.killed() && rf.state == Leader && rf.currentTerm == term {
		for i:=0; i<len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := AppendEntriesReply{}
				args := AppendEntriesArgs{}
				args.Term = term
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if args.PrevLogIndex != 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].CommandTerm
				}
				args.Entries = make([] Entry, 0)
				if len(rf.log) >= rf.nextIndex[i] {
					args.Entries = append(args.Entries, rf.log[rf.nextIndex[i] - 1:]...)
				}
				args.LeaderCommit = rf.commitIndex
				DPrintf("%v sends AppendEntry to %v", rf.me, i)
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					if rf.state == Leader && rf.currentTerm < reply.Term {
						rf.state = Follower
						go rf.operateFollower()
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						rf.nextIndex[i] += len(args.Entries)
						rf.matchIndex[i] = rf.nextIndex[i] - 1
					} else {
						switch reply.RejectReason {
						case ByTerm: break
						case ByLength: rf.nextIndex[i] = reply.RejectionLength + 1
						case ByConflict: {
							for rf.nextIndex[i] > 1 &&
								rf.nextIndex[i] > reply.RejectionIndex &&
								rf.log[rf.nextIndex[i]-2].CommandTerm != reply.RejectionTerm {
								rf.nextIndex[i] --
							}
						}
						}
					}
					rf.mu.Unlock()
				}
			} (i)
		}
		rf.mu.Unlock()
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
	var lastLogIndex, lastLogTerm int
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].CommandIndex
		lastLogTerm = rf.log[len(rf.log)-1].CommandTerm
	}
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
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}
			DPrintf("%v requests vote from %v", rf.me, i)
			ok := rf.sendRequestVote(i, &args, &reply)
			c <- ok && reply.VoteGranted
		} (i)
	}
	rf.mu.Unlock()
	votesReceived := 0
	done := false
	electionExpires := time.After(randElectionWaitTime())
	for {
		select {
			case vote := <-c: {
				if vote {
					votesReceived++
				}
				done = votesReceived >= votesNeeded
			}
			case <- electionExpires:
				done = true
		}
		if done {
			break
		}
	}
	rf.mu.Lock()
	if votesReceived >= votesNeeded && rf.state == Candidate && rf.currentTerm == term {
		DPrintf("%v elected leader", rf.me)
		rf.state = Leader
		go rf.operateLeader()
	}
}

//
// A long running goroutine that does 2 things:
// 1. if state is leader, check and increment commitIndex
// 2. for all states, apply message before and at commitIndex
//
func (rf *Raft) operateCommitAndApply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			ints := make([]int, len(rf.matchIndex))
			copy(ints, rf.matchIndex)
			sort.Ints(ints)
			target := ints[len(rf.matchIndex) / 2]
			if target > rf.commitIndex {
				rf.commitIndex = target
			}
		}
		for rf.lastApplied < rf.commitIndex {
			DPrintf("%v applied %v", rf.me, rf.lastApplied)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].CommandIndex,
			}
			rf.lastApplied ++
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
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

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = make([]Entry, 0)

	go rf.operateFollower()
	go rf.operateCommitAndApply()
	DPrintf("%v started", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
