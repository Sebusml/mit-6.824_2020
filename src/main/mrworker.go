package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"mit6.824/src/mr"
	mrapp "mit6.824/src/mrapps/wc" // Change this package to run different apps
)

func main() {
	mapf := mrapp.Map
	reducef := mrapp.Reduce

	mr.Worker(mapf, reducef)
}
