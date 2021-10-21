package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"mit6.824/src/mr"
)

// TODO: Experiment with other MapReduce functions
import mrapp "mit6.824/src/mrapps/wc"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Refactored project so that we can debug and place break points with GoLand :)
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

	mapf := mrapp.Map
	reducef := mrapp.Reduce

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf(err.Error())
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	println(ofile.Name())
}
