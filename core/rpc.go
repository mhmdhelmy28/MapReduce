package core

//
// RPC definitions.
//

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	None TaskType = iota
	Map
	Reduce
	Sleep
	Exit
)

type FinishedTaskArgs struct {
	Type  TaskType
	Id    int
	Files []string
}

type TaskReply struct {
	Type            TaskType
	Id              int
	Files           []string
	NoOfReduceTasks int
}

// Add your RPC definitions here.
