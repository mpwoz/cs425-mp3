package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {

	fmt.Println("Please enter your input")
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
			//insert(key, val)
		case "update":
			//update(key, val)
		case "remove":
			//remove(key)
		case "lookup":
			fmt.Println("Looking Up")
			//lookup(key)
		case "leave":
			fmt.Println("Leaving Group")
			fmt.Println(key)
			fmt.Println(val)
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic("Scanning stdin", err)
	}

}
