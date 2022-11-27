package main

import (
	"fmt"
	mr "github.com/mhmdhelmy28/mr/core"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	var kva []mr.KeyValue
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: main inputfiles...\n")
		os.Exit(1)
	}
	nReduce, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Number of reduce tasks should be an integer")
	}
	nWorker, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal("Number of workers should be an integer")
	}
	fileNames := os.Args[3:]
	mr.MakeMaster(fileNames, nReduce)
	for i := 0; i < nWorker; i++ {
		mr.Worker(Map, Reduce)
	}

}