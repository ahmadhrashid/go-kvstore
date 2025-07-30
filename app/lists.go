package main

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"
)

type blpopRequest struct {
	conn     io.Writer
	listKeys []string
	timeout  time.Duration
	done     chan struct{} // signal when handled
}

var (
	blockedClients   = make([]*blpopRequest, 0)
	blockedClientsMu sync.Mutex
)

func handleRPush(conn io.Writer, commands []string) {
	if len(commands) < 3 {
		conn.Write([]byte("-ERR incorrect number of arguments for RPUSH\r\n"))
		return
	}
	key := commands[1]
	for _, val := range commands[2:] {
		lists[key] = append(lists[key], val)
	}
	fmt.Fprintf(conn, ":%d\r\n", len(lists[key]))
	wakeUpBlockedClients(key)
}

func handleLRange(conn io.Writer, commands []string) {
	// LRANGE key start stop
	if len(commands) != 4 {
		fmt.Fprint(conn, "-ERR incorrect number of arguments for LRANGE\r\n")
		return
	}
	key := commands[1]
	list := lists[key] // nil slice if key doesn't exist

	// parse start and stop
	start, err := strconv.Atoi(commands[2])
	if err != nil {
		fmt.Fprint(conn, "-ERR start index must be an integer\r\n")
		return
	}
	stop, err := strconv.Atoi(commands[3])
	if err != nil {
		fmt.Fprint(conn, "-ERR stop index must be an integer\r\n")
		return
	}

	n := len(list)
	// convert negative indexes
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	// clamp to [0..n-1]
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		fmt.Fprint(conn, "*0\r\n")
		return
	}
	if start >= n {
		fmt.Fprint(conn, "*0\r\n")
		return
	}
	if stop >= n {
		stop = n - 1
	}
	// invalid range
	if start > stop {
		fmt.Fprint(conn, "*0\r\n")
		return
	}

	// slice is inclusive of stop
	slice := list[start : stop+1]
	// write as a RESP array
	fmt.Fprint(conn, encodeRESPArray(slice...))
}

func handleLPush(conn io.Writer, commands []string) {
	if len(commands) < 3 {
		conn.Write([]byte("-ERR incorrect number of arguments for LPUSH\r\n"))
		return
	}
	key := commands[1]
	for _, val := range commands[2:] {
		lists[key] = append([]string{val}, lists[key]...)
	}
	fmt.Fprintf(conn, ":%d\r\n", len(lists[key]))
	wakeUpBlockedClients(key)
}

func handleLLen(conn io.Writer, commands []string) {
	if len(commands) != 2 {
		fmt.Fprint(conn, "-ERR incorrect number of arguments for LLEN\r\n")
		return
	}
	fmt.Fprintf(conn, ":%d\r\n", len(lists[commands[1]]))
}

func handleLPop(conn io.Writer, commands []string) {
	if len(commands) != 2 && len(commands) != 3 {
		fmt.Fprintf(conn, "-ERR incorrect number of arguments for LPOP\r\n")
		return
	}
	if len(commands) == 2 {
		handleSingleLPPOP(conn, commands)
	} else {
		handleMultiLPPOP(conn, commands)
	}

}

func handleSingleLPPOP(conn io.Writer, commands []string) {

	key := commands[1]
	if list, exists := lists[key]; !exists || len(list) == 0 {
		fmt.Fprint(conn, "$-1\r\n")
		return
	}
	val := lists[key][0]
	lists[key] = lists[key][1:]
	fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
}

func handleMultiLPPOP(conn io.Writer, commands []string) {
	key := commands[1]
	numElems, err := strconv.Atoi(commands[2])
	if err != nil {
		fmt.Fprint(conn, "-ERR third argument must be int\r\n")
		return
	}
	list, exists := lists[key]
	if !exists || len(list) <= numElems || len(list) == 0 {
		lists[key] = nil
		resp := encodeRESPArray(list...)
		fmt.Fprint(conn, resp)
		return
	}

	popped := list[:numElems]
	lists[key] = list[numElems:]
	resp := encodeRESPArray(popped...)
	fmt.Fprint(conn, resp)
}

func handleBLPop(conn io.Writer, commands []string) {
	if len(commands) < 3 {
		fmt.Fprint(conn, "-ERR wrong number of arguments for 'blpop'\r\n")
		return
	}

	listKeys := commands[1 : len(commands)-1]
	timeoutStr := commands[len(commands)-1]
	timeoutFloat, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil || timeoutFloat < 0 {
		fmt.Fprint(conn, "-ERR timeout must be a non-negative number\r\n")
		return
	}

	// Try popping immediately
	for _, key := range listKeys {
		list, exists := lists[key]
		if exists && len(list) > 0 {
			val := list[0]
			lists[key] = list[1:]
			resp := encodeRESPArray(key, val)
			fmt.Fprint(conn, resp)
			return
		}
	}

	// If nothing available, block
	req := &blpopRequest{
		conn:     conn,
		listKeys: listKeys,
		timeout:  time.Duration(timeoutFloat * float64(time.Second)),
		done:     make(chan struct{}),
	}

	blockedClientsMu.Lock()
	blockedClients = append(blockedClients, req)
	blockedClientsMu.Unlock()

	// Handle timeout
	go func() {
		if timeoutFloat > 0 {
			select {
			case <-time.After(req.timeout):
				blockedClientsMu.Lock()
				for i, r := range blockedClients {
					if r == req {
						blockedClients = append(blockedClients[:i], blockedClients[i+1:]...)
						break
					}
				}
				blockedClientsMu.Unlock()

				fmt.Fprint(conn, "$-1\r\n")
			case <-req.done:
				// already handled
			}
		}
	}()
}

func wakeUpBlockedClients(key string) {
	blockedClientsMu.Lock()
	defer blockedClientsMu.Unlock()

	for i := 0; i < len(blockedClients); {
		req := blockedClients[i]
		for _, k := range req.listKeys {
			if k == key {
				list := lists[k]
				if len(list) > 0 {
					val := list[0]
					lists[k] = list[1:]
					resp := encodeRESPArray(k, val)
					fmt.Fprint(req.conn, resp)

					close(req.done)
					blockedClients = append(blockedClients[:i], blockedClients[i+1:]...)
					goto outer
				}
			}
		}
		i++
	outer:
	}
}
