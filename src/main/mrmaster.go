package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import "mit6.824/src/mr"
import "time"

func main() {
	files := []string{
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-being_ernest.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-dorian_gray.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-frankenstein.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-grimm.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-huckleberry_finn.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-metamorphosis.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-sherlock_holmes.txt",
		"/home/sebastian/courses/mit/mit6.824/src/main/pg-tom_sawyer.txt",
	}

	m := mr.MakeMaster(files, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
