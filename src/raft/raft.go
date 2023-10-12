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
	//"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

type RaftState int

const (
        FOLLOWER RaftState = iota
        CANDIDATE
        LEADER
)

type LogEntry struct {
	Command interface{}
	Term    int
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

        currentTerm       int
        votedFor          int
        logEntries        []LogEntry // pair of cmd and term number
        commitIndex       int
        lastApplied       int
	lastLogIndex 	  int
        nextIndex         []int
        matchIndex        []int
        state             RaftState
        electionTimeout   time.Duration
        electionResetTime time.Time
	applyMsg     chan ApplyMsg
        // Your data here (2A, 2B, 2C).
        // Look at the paper's Figure 2 for a description of what
        // state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
        // Your code here (2A).
        rf.mu.Lock()
        defer rf.mu.Unlock()
        var term int
        var isleader bool
        term = rf.currentTerm
        if rf.state == LEADER {
                isleader = true
        } else {
                isleader = false
        }
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	LastTerm   int
	LastIndex  int
	LastLen    int
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
                reply.Term = rf.currentTerm
                rf.electionResetTime = time.Now()
		return
	}
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
                rf.votedFor = -1
		rf.state = FOLLOWER
	}
	reply.Term = args.Term
	if len(rf.logEntries)-1 > 0 {
		lastLogTerm := rf.logEntries[len(rf.logEntries)-2].Term
		if lastLogTerm > args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && len(rf.logEntries)-1 > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
	}
	rf.state = FOLLOWER
	rf.electionResetTime = time.Now()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.LastTerm = -1
	reply.LastIndex = -1
	reply.LastLen = len(rf.logEntries)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if len(rf.logEntries) < args.PrevLogIndex+1 {
		return
	}

	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.LastTerm = rf.logEntries[args.PrevLogIndex].Term
		for i, v := range rf.logEntries {
			if v.Term == reply.LastTerm {
				reply.LastIndex = i
				break
			}
		}
		return
	}

	index := 0
	for ; index < len(args.Entry); index++ {
		currentIndex := args.PrevLogIndex + 1 + index
		if currentIndex > len(rf.logEntries)-1 {
			break
		}
		if rf.logEntries[currentIndex].Term != args.Entry[index].Term {
			rf.logEntries = rf.logEntries[:currentIndex]
			rf.lastLogIndex = len(rf.logEntries) - 1
			break
		}
	}

	reply.Success = true
	rf.electionResetTime = time.Now()
	if len(args.Entry) > 0 {
		rf.logEntries = append(rf.logEntries, args.Entry[index:]...)
		rf.lastLogIndex = len(rf.logEntries) - 1
	}
	if args.LeaderCommit > rf.commitIndex {
		min_idx := args.LeaderCommit
		if min_idx > rf.lastLogIndex {
			min_idx = rf.lastLogIndex
		}
		for i := rf.commitIndex + 1; i <= min_idx; i++ {
			rf.commitIndex = i
			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[i].Command,
				CommandIndex: i,
			}
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		isLeader = false
	}
	if !isLeader {
		return 0, 0, false
	}

	term = rf.currentTerm
	rf.logEntries = append(rf.logEntries, LogEntry{
		Command: command,
		Term:    term,
	})
	rf.lastLogIndex = len(rf.logEntries) - 1
	index = len(rf.logEntries) - 1
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

func randElectionTimeout() time.Duration {
        timeout := 500 + rand.Intn(1000-500)
        return time.Duration(timeout) * time.Millisecond
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
	rf.currentTerm = 0
        rf.votedFor = -1
        rf.logEntries = []LogEntry{{Command: nil, Term: 0}}
        rf.commitIndex = 0
        rf.lastApplied = 0
        rf.nextIndex = make([]int, len(rf.peers))
        rf.matchIndex = make([]int, len(rf.peers))
        rf.state = FOLLOWER
	rf.electionTimeout = randElectionTimeout()
	rf.electionResetTime = time.Now()
	rf.applyMsg = applyCh

	// initialize from state persisted before a crash
	// Need to write a go routine for requestVoteRpc
	rf.readPersist(persister.ReadRaftState())
	go rf.startElectionTimer()

	return rf
}

func (rf *Raft) startElectionTimer() {
	for true {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == FOLLOWER {
			time.Sleep(rf.electionTimeout)
                        rf.mu.Lock()
                        electionResetTime := rf.electionResetTime
                        rf.mu.Unlock()
                        timeDiff := time.Now().Sub(electionResetTime).Milliseconds()
                        if timeDiff >= rf.electionTimeout.Milliseconds() {
                                rf.mu.Lock()
                                rf.state = CANDIDATE
                                rf.votedFor = -1
                                rf.currentTerm += 1
                                rf.mu.Unlock()
                        }
		} else if state == CANDIDATE {
			rf.CandidateOps()
		} else if state == LEADER {
			rf.LeaderOps()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) CandidateOps() {
	rf.electionTimeout = randElectionTimeout()
	start := time.Now()
	voteReceived := 0

	rf.mu.Lock()
	rf.votedFor = rf.me
        total := len(rf.peers)
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logEntries) - 1,
		LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
	}
	rf.mu.Unlock()
	requestVoteReply := RequestVoteReply{}
	for i := 0; i < total; i++ {
		if i == rf.me {
			rf.mu.Lock()
			voteReceived++
			rf.mu.Unlock()
			continue
		}

		go func(i int) { // New thread for each requestVote.
			ok := rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if requestVoteReply.VoteGranted {
					voteReceived++
				} else {
					if requestVoteArgs.Term < requestVoteReply.Term {
						rf.state = FOLLOWER
					}
				}
			}
		}(i)
	}

	for {
		rf.mu.Lock()
		if voteReceived > len(rf.peers)/2 {
			break
		}

		if time.Now().Sub(start).Milliseconds() >= rf.electionTimeout.Milliseconds() {
			rf.state = FOLLOWER
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	if rf.state == CANDIDATE && voteReceived > len(rf.peers)/2 { // Won election
		rf.state = LEADER
		for i := 0; i < total; i++ {
			rf.nextIndex[i] = rf.lastLogIndex + 1
		}
	} else {
		rf.state = FOLLOWER
	}
	rf.mu.Unlock()
}

func (rf *Raft) LeaderOps() {
	rf.mu.Lock()
	total := len(rf.peers)
	nextIndex := rf.nextIndex
	matchIndex := rf.matchIndex
	nextIndex[rf.me] = rf.lastLogIndex + 1
	matchIndex[rf.me] = rf.lastLogIndex
	logEntries := rf.logEntries
	for n := rf.commitIndex + 1; n <= rf.lastLogIndex; n++ {
		voteReceived := 0
		for i := 0; i < total; i++ {
			if matchIndex[i] >= n && logEntries[n].Term == rf.currentTerm {
				voteReceived++
			}
		}

		if voteReceived > total/2 {
			for i := rf.commitIndex + 1; i <= n; i++ {
				rf.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      logEntries[i].Command,
					CommandIndex: i,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
		}
	}
	rf.mu.Unlock()

	for i := 0; i < total; i++ {
		rf.mu.Lock()
		appendEntriesArgs := AppendEntriesArgs{}
		appendEntriesArgs.Term = rf.currentTerm
		prevLogIndex := nextIndex[i] - 1
		appendEntriesArgs.PrevLogIndex = prevLogIndex
		appendEntriesArgs.PrevLogTerm = rf.logEntries[prevLogIndex].Term
		appendEntriesArgs.LeaderCommit = rf.commitIndex
		appendEntriesArgs.LeaderId = rf.me
		if nextIndex[i] <= rf.lastLogIndex {
			appendEntriesArgs.Entry = rf.logEntries[prevLogIndex + 1 : rf.lastLogIndex + 1]
		}
		appendEntriesReply := AppendEntriesReply{}
		rf.mu.Unlock()
		
		if i == rf.me {
			continue
		}

		go func(i int) {
			ok := rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
			rf.mu.Lock()
			if ok {
				if appendEntriesReply.Success {
					rf.nextIndex[i] = rf.nextIndex[i]+len(appendEntriesArgs.Entry)
					if rf.nextIndex[i] > rf.lastLogIndex+1 {
						rf.nextIndex[i] = rf.lastLogIndex+1
					}
					rf.matchIndex[i] = prevLogIndex + len(appendEntriesArgs.Entry)
				} else {
					if appendEntriesReply.Term > appendEntriesArgs.Term {
						rf.state = FOLLOWER
						rf.mu.Unlock()
						return
					}
					if appendEntriesReply.LastTerm == -1 {
						rf.nextIndex[i] = appendEntriesReply.LastLen
						rf.mu.Unlock()
						return
					}
					index := -1
					for i, v := range rf.logEntries {
						if v.Term == appendEntriesReply.LastTerm {
							index = i
						}
					}
					rf.nextIndex[i] = index
					if index == -1 {
						rf.nextIndex[i] = appendEntriesReply.LastIndex
					}
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}
