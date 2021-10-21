# MIT Distributed Systems class [2020]

This repo contains implementation for the labs and some other exercises/homeworks.

Many files were modified to enable running and debugging with GoLang IDE. 

[http://nil.csail.mit.edu/6.824](http://nil.csail.mit.edu/6.824/2020/schedule.html)

## Lab 1
Implement a MapReduce application running locally.

1. Master node runs mainly two go-routines, one to check stale/failed tasks, and another, to run the RPC server. 
   Note that more go-routines are created for handling each RPC call.
2. The server exposes two methods: 
   [Master.GetTask](https://github.com/Sebusml/mit-6.824_2020/blob/main/src/mr/master.go#L101) and
   [Master.CompletedTask](https://github.com/Sebusml/mit-6.824_2020/blob/main/src/mr/master.go#L59). 
3. Workers have one go-routine that constantly 
   [calls](https://github.com/Sebusml/mit-6.824_2020/blob/main/src/mr/worker.go#L63) the Master node to get new tasks.
   1. If no tasks are available at the moment, worker will wait and ask later.
   2. If all tasks are completed or Master is down, workers will exit.
   3. Workers notify the master node that a task is completed by calling `Master.CompletedTask`. Master node replies
      to either exit or to ask for a new task.

Entry files are:
- [master](https://github.com/Sebusml/mit-6.824_2020/blob/main/src/main/mrmaster.go)
- [worker](https://github.com/Sebusml/mit-6.824_2020/blob/main/src/main/mrworker.go)

They can easily be executed/debugged with the IDE.

## Notes
- I didn't submit any lab, since I wasn't enrolled, but I respected deadlines :). 
- Repository was not created with a fork so the git history looks off.

## TODOs
- Because many files were changed, tests scripts won't run. However, the labs were tested locally without
the package changes.
