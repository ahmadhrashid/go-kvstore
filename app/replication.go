package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func handlePSYNC(conn net.Conn, commands []string) {
	response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", masterReplID)
	conn.Write([]byte(response))

	var emptyRDB = []byte{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
		0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72,
		0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36,
		0xFF, 0x89, 0x3B, 0xB7, 0x4E, 0xF8, 0x0F, 0x77, 0x19,
	}
	rdbHeader := fmt.Sprintf("$%d\r\n", len(emptyRDB))
	conn.Write([]byte(rdbHeader))
	conn.Write(emptyRDB)
	replicaConnsMu.Lock()
	replicaConns[conn] = struct{}{}
	replicaConnsMu.Unlock()

	go func(c net.Conn) {
		defer c.Close()
		fmt.Printf("[ACK-READER] Started for %v\n", c.RemoteAddr())
		reader := bufio.NewReader(c)
		for {
			resp, err := parseRESPArray(reader)
			if err != nil {
				fmt.Printf("[ACK-READER] Connection closed for replica %v: %v\n", c.RemoteAddr(), err)
				return
			}
			fmt.Printf("[ACK-READER] Received from replica %v: %v\n", c.RemoteAddr(), resp)
			if len(resp) >= 3 && strings.ToUpper(resp[0]) == "REPLCONF" && strings.ToUpper(resp[1]) == "ACK" {
				offset, _ := strconv.ParseInt(resp[2], 10, 64)
				fmt.Printf("[ACK-READER] Parsed ACK offset %d from replica %v\n", offset, c.RemoteAddr())
				waitMu.Lock()
				replicaOffsets[c] = offset
				fmt.Printf("[ACK-READER] Updated replicaOffsets: %v\n", replicaOffsets)
				for i := 0; i < len(pendingWaits); {
					count := 0
					for _, ro := range replicaOffsets {
						if ro >= pendingWaits[i].reqOffset {
							count++
						}
					}
					if count >= pendingWaits[i].numNeeded {
						fmt.Printf("[ACK-READER] Fulfilling WAIT: reqOffset=%d, numNeeded=%d, count=%d\n", pendingWaits[i].reqOffset, pendingWaits[i].numNeeded, count)
						pendingWaits[i].done <- count
						pendingWaits = append(pendingWaits[:i], pendingWaits[i+1:]...)
					} else {
						i++
					}
				}
				fmt.Printf("[ACK-READER] Pending waits after check: %v\n", pendingWaits)
				waitMu.Unlock()
			}
		}
	}(conn)
}

func handleWait(conn io.Writer, commands []string) {
	if len(commands) != 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'wait' command\r\n"))
		return
	}
	numReplicas, err1 := strconv.Atoi(commands[1])
	timeoutMs, err2 := strconv.Atoi(commands[2])
	if err1 != nil || err2 != nil {
		conn.Write([]byte("-ERR invalid arguments for 'wait' command\r\n"))
		return
	}

	replicaConnsMu.Lock()
	for c := range replicaConns {
		c.Write([]byte(encodeRESPArray("REPLCONF", "GETACK", "*")))
	}
	replicaConnsMu.Unlock()

	waitMu.Lock()
	reqOffset := masterOffset
	var acked int
	if reqOffset == 0 {
		replicaConnsMu.Lock()
		acked = len(replicaConns)
		fmt.Printf("REQOFFSEST WAS 0. SENDING %d ACKS", acked)
		replicaConnsMu.Unlock()
		waitMu.Unlock()
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", acked)))
		return
	} else {
		for _, off := range replicaOffsets {
			if off >= reqOffset {
				acked++
			}
		}
	}
	fmt.Printf("[WAIT] reqOffset=%d, numReplicas=%d, acked=%d, replicaOffsets=%v\n", reqOffset, numReplicas, acked, replicaOffsets)
	fmt.Printf("[WAIT] replicaOffsets keys: ")
	for k := range replicaOffsets {
		fmt.Printf("%v ", k.RemoteAddr())
	}
	fmt.Println()
	waitMu.Unlock()

	if acked >= numReplicas {
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", acked)))
		return
	}

	fmt.Println("[WAIT] waiting for more ACKs")
	done := make(chan int, 1)
	wr := waitReq{reqOffset, numReplicas, done, time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)}
	waitMu.Lock()
	pendingWaits = append(pendingWaits, wr)
	waitMu.Unlock()

	select {
	case n := <-done:
		fmt.Println("[WAIT] done")
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		fmt.Println("[WAIT] timeout")
		waitMu.Lock()
		for i, w := range pendingWaits {
			if w == wr {
				fmt.Printf("[WAIT] removing wr %v\n", wr)
				pendingWaits = append(pendingWaits[:i], pendingWaits[i+1:]...)
				break
			}
		}
		acked := 0
		fmt.Println("[WAIT] Recounting ACKs")
		for _, off := range replicaOffsets {
			if off >= reqOffset {
				acked++
				fmt.Printf("[WAIT] Counted %d ACKs\n", acked)
			}
		}
		waitMu.Unlock()
		fmt.Printf("[WAIT] counted %d ACKs\n", acked)
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", acked)))
	}
}

// connectToMasterAndHandshake establishes connection to master and performs handshake
func connectToMasterAndHandshake(replicaof string) {
	parts := strings.Fields(replicaof)
	if len(parts) != 2 {
		fmt.Println("Invalid --replicaof format. Use: --replicaof <host> <port>")
		os.Exit(1)
	}
	masterHost := parts[0]
	masterPort := parts[1]
	fmt.Printf("Connecting to master at %s:%s...\n", masterHost, masterPort)
	conn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		os.Exit(1)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	offset := 0

	ping := encodeRESPArray("PING")
	_, err = conn.Write([]byte(ping))
	if err != nil {
		fmt.Println("Failed to send PING to master:", err)
		os.Exit(1)
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read PING response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after PING: %s", resp)

	replconf := encodeRESPArray("REPLCONF", "listening-port", port)
	_, err = conn.Write([]byte(replconf))
	if err != nil {
		fmt.Println("Failed to send REPLCONF to master")
		os.Exit(1)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read REPLCONF response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after REPLCONF listening-port: %s", resp)

	replconf = encodeRESPArray("REPLCONF", "capa", "psync2")
	_, err = conn.Write([]byte(replconf))
	if err != nil {
		fmt.Println("Failed to send REPLCONF to master")
		os.Exit(1)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read REPLCONF capa response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after REPLCONF capa: %s", resp)

	psync := encodeRESPArray("PSYNC", "?", "-1")
	_, err = conn.Write([]byte(psync))
	if err != nil {
		fmt.Println("Failed to send PSYNC to master")
		os.Exit(1)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read PSYNC response from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Received after PSYNC: %s", resp)

	rdbHeader, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read RDB header from master:", err)
		os.Exit(1)
	}
	if !strings.HasPrefix(rdbHeader, "$") {
		fmt.Println("Invalid RDB header from master:", rdbHeader)
		os.Exit(1)
	}
	lengthStr := strings.TrimSpace(rdbHeader[1:])
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		fmt.Println("Invalid RDB length from master:", lengthStr)
		os.Exit(1)
	}
	rdbData := make([]byte, length)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		fmt.Println("Failed to read RDB data from master:", err)
		os.Exit(1)
	}
	fmt.Printf("Read RDB file of %d bytes from master\n", length)

	for {
		peekBytes, err := reader.Peek(10)
		if err != nil {
			fmt.Println("Error peeking at next bytes:", err)
			return
		}
		fmt.Printf("About to read bytes: %q\n", peekBytes)

		commands, err := parseRESPArray(reader)
		if err != nil {
			if err.Error() == "EOF" || strings.Contains(err.Error(), "connection") {
				fmt.Println("No more propagated commands from master")
				return
			}
			fmt.Println("Error reading propagated command from master:", err)
			return
		}
		if len(commands) == 0 {
			continue
		}

		commandSize := len(encodeRESPArray(commands...))
		fmt.Printf("Received propagated command: %v (size: %d bytes)\n", commands, commandSize)

		command := strings.ToUpper(commands[0])
		switch command {
		case "SET":
			if len(commands) >= 3 {
				db[commands[1]] = commands[2]
				if len(commands) == 5 && strings.ToUpper(commands[3]) == "PX" {
					dbExpireTime, err := strconv.Atoi(commands[4])
					if err != nil {
						fmt.Println("Invalid expiration time in propagated command:", err)
						continue
					}
					expiryDB[commands[1]] = time.Now().Add(time.Duration(dbExpireTime) * time.Millisecond)
				}
				fmt.Printf("Applied propagated SET %s = %s\n", commands[1], commands[2])
			}
		case "REPLCONF":
			if len(commands) >= 3 && strings.ToUpper(commands[1]) == "GETACK" && commands[2] == "*" {
				resp := encodeRESPArray("REPLCONF", "ACK", fmt.Sprintf("%d", offset))
				_, err = conn.Write([]byte(resp))
				if err != nil {
					fmt.Println("Failed to send GETACK response to master")
					os.Exit(1)
				}
				fmt.Printf("Sent REPLCONF ACK %d\n", offset)
			}
		case "PING":
			fmt.Printf("Received PING from master\n")
		default:
			fmt.Printf("Received propagated command: %s\n", command)
		}

		offset += commandSize
		fmt.Printf("Updated offset to: %d\n", offset)
	}
}
