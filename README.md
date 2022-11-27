# MapReduce
This is a simple MapReduce implementation in Go that supports multiple workers at the same time being coordinated by one coordinator/master.




# Usage:
- write your own map and reduce functions in main.go or use the default which helps you to do a word count in some given files.
-  Run
```
go run main.go <noOfReduceTasks:int> <noOfWorkers:int> <files to be operated on>
go run main.go 2 2 test1 test2
```
