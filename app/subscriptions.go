package main

import (
	"fmt"
	"io"
	"sync"
)

var clientSubscriptions = make(map[io.Writer]map[string]struct{})
var subMu sync.Mutex

func handleSubscribe(conn io.Writer, commands []string) {
	if len(commands) != 2 {
		fmt.Fprint(conn, "-ERR Invalid number of arguments for SUBSCRIBE\r\n")
		return
	}

	channel := commands[1]

	subMu.Lock()
	defer subMu.Unlock()

	if _, ok := clientSubscriptions[conn]; !ok {
		clientSubscriptions[conn] = make(map[string]struct{})
	}

	// Add to client-specific set of subscriptions
	clientSubs := clientSubscriptions[conn]
	clientSubs[channel] = struct{}{} // idempotent

	fmt.Fprintf(conn, "*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
		len(channel), channel, len(clientSubs))
	subscribeMode = true
}

// func handleSubscribeMode(conn io.Writer, commands []string) {

// }

func handleDisconnect(conn io.Writer) {
	subMu.Lock()
	delete(clientSubscriptions, conn)
	subMu.Unlock()
}
