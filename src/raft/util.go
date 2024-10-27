package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Assert(condition bool, msg string) {
	if Debug && !condition {
		log.Fatalf("Assertion failed: %s", msg)
	}
}