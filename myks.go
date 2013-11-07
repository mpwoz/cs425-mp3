/*
 The main command-line program for joining the group and running queries on the hashtable
 Once started, allows user to type commands like 'insert k,v'
 as well as querying the ring
 */

package main

import (
  "bufio"
  "os"
  "log"
  "strings"
)

// The operations
func insert(key, val string) {
}

func update(key, val string) {
}

func remove(key string) {
}

func lookup(key string) {
}


func main() {

  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := strings.TrimSpace(scanner.Text())
    words := strings.Split(line, " ")
    var key, val string
    key = words[1]
    if len(words) > 2 {
      val = words[2]
    }
    switch words[0] {
    case "insert":
      insert(key, val)
    case "update":
      update(key, val)
    case "remove":
      remove(key)
    case "lookup":
      lookup(key)
    }
  }

  if err := scanner.Err(); err != nil {
    log.Panic("Scanning stdin", err)
  }

}



