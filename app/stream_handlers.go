package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func handleXAdd(conn io.Writer, commands []string) {
	if len(commands) < 5 || (len(commands)-3)%2 != 0 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
		return
	}
	key, id := commands[1], commands[2]
	fields := make(map[string]string, (len(commands)-3)/2)
	for i := 3; i < len(commands); i += 2 {
		fields[commands[i]] = commands[i+1]
	}

	parts := strings.Split(id, "-")
	if len(parts) == 1 && parts[0] == "*" {
		generateEntryID(&id, key)
	} else if len(parts) != 2 {
		conn.Write([]byte("-ERR Invalid ID"))
		return
	} else if parts[1] == "*" {
		generateSequenceNum(&id, key)
	}

	streamsMu.Lock()
	_, exists := streams[key]
	if id <= "0-0" {
		streamsMu.Unlock()
		conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
	} else if exists && id <= streams[key][len(streams[key])-1].ID {
		streamsMu.Unlock()
		conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
	} else {
		streams[key] = append(streams[key], streamEntry{
			ID:     id,
			Fields: fields,
		})
		streamsMu.Unlock()

		notifyWaitingXReads(key)
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)))
	}
}

func handleXRange(conn io.Writer, commands []string) {
	if len(commands) != 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
		return
	}
	key, startID, endID := commands[1], commands[2], commands[3]
	streamList := streams[key]

	startFlag := startID == "-"
	endFlag := endID == "+"

	var resp strings.Builder
	var inRange []streamEntry
	for _, entry := range streamList {
		if (startID <= entry.ID || startFlag) && (entry.ID <= endID || endFlag) {
			inRange = append(inRange, entry)
		}
	}

	resp.WriteString(fmt.Sprintf("*%d\r\n", len(inRange)))
	for _, entry := range inRange {
		resp.WriteString("*2\r\n")
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
		for k, v := range entry.Fields {
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
		}
	}
	conn.Write([]byte(resp.String()))
}

func handleBlockingXRead(conn io.Writer, streamKeys []string, lastIDs []string, timeoutMs int) {
	// First check if there are already new entries
	hasNewData := false
	for i, streamKey := range streamKeys {
		if hasNewEntries(streamKey, lastIDs[i]) {
			hasNewData = true
			break
		}
	}

	if hasNewData {
		// Return data immediately
		handleNonBlockingXRead(conn, streamKeys, lastIDs)
		return
	}

	// Set up blocking wait
	var deadline time.Time
	if timeoutMs > 0 {
		timeout := time.Duration(timeoutMs) * time.Millisecond
		deadline = time.Now().Add(timeout)
	} else {
		// For timeoutMs == 0, set a far future deadline (effectively no timeout)
		deadline = time.Now().AddDate(100, 0, 0) // 100 years from now
	}

	// Create notification channels for all streams
	notifiers := make([]chan struct{}, len(streamKeys))
	for i, streamKey := range streamKeys {
		notifiers[i] = getStreamNotifier(streamKey)
	}

	// Create done channel for this XREAD command
	done := make(chan bool, 1)

	// Add to waiting XREAD commands
	waitingXReadsMu.Lock()
	waitingXRead := waitingXRead{
		conn:    conn,
		streams: streamKeys,
		lastIDs: lastIDs,
		timeout: deadline,
		done:    done,
	}
	waitingXReads = append(waitingXReads, waitingXRead)
	waitingXReadsMu.Unlock()

	// Start goroutine to wait for notifications or timeout
	go func() {
		// Create a channel to receive notifications from any stream
		notificationChan := make(chan struct{})

		// Start goroutines to listen to all stream notifiers
		for _, notifier := range notifiers {
			go func(n chan struct{}) {
				for {
					select {
					case <-n:
						select {
						case notificationChan <- struct{}{}:
						default:
						}
					case <-done:
						return
					}
				}
			}(notifier)
		}

		// Handle timeout only if timeoutMs > 0
		if timeoutMs > 0 {
			timeout := time.Duration(timeoutMs) * time.Millisecond
			timer := time.NewTimer(timeout)
			defer timer.Stop()

			select {
			case <-done:
				// Another goroutine found data, we're done
				return
			case <-timer.C:
				// Timeout reached
				waitingXReadsMu.Lock()
				// Remove from waiting list
				for i, wr := range waitingXReads {
					if wr.conn == conn {
						waitingXReads = append(waitingXReads[:i], waitingXReads[i+1:]...)
						break
					}
				}
				waitingXReadsMu.Unlock()

				// Send null response
				conn.Write([]byte("$-1\r\n"))
				return
			case <-notificationChan:
				// Check if we have new data
				hasNewData := false
				for i, streamKey := range streamKeys {
					if hasNewEntries(streamKey, lastIDs[i]) {
						hasNewData = true
						break
					}
				}

				if hasNewData {
					// Remove from waiting list
					waitingXReadsMu.Lock()
					for i, wr := range waitingXReads {
						if wr.conn == conn {
							waitingXReads = append(waitingXReads[:i], waitingXReads[i+1:]...)
							break
						}
					}
					waitingXReadsMu.Unlock()

					// Send data
					handleNonBlockingXRead(conn, streamKeys, lastIDs)
					return
				}
			}
		} else {
			// No timeout - wait indefinitely
			select {
			case <-done:
				// Another goroutine found data, we're done
				return
			case <-notificationChan:
				// Check if we have new data
				hasNewData := false
				for i, streamKey := range streamKeys {
					if hasNewEntries(streamKey, lastIDs[i]) {
						hasNewData = true
						break
					}
				}

				if hasNewData {
					// Remove from waiting list
					waitingXReadsMu.Lock()
					for i, wr := range waitingXReads {
						if wr.conn == conn {
							waitingXReads = append(waitingXReads[:i], waitingXReads[i+1:]...)
							break
						}
					}
					waitingXReadsMu.Unlock()

					// Send data
					handleNonBlockingXRead(conn, streamKeys, lastIDs)
					return
				}
			}
		}
	}()
}

func handleNonBlockingXRead(conn io.Writer, streamKeys []string, lastIDs []string) {
	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("*%d\r\n", len(streamKeys)))

	for i, streamKey := range streamKeys {
		entries := getNewEntries(streamKey, lastIDs[i])

		if len(entries) == 0 {
			resp.WriteString("*0\r\n")
			continue
		}

		resp.WriteString("*2\r\n")
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(streamKey), streamKey))
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(entries))) // number of entries

		for _, entry := range entries {
			resp.WriteString("*2\r\n") // [id, [fields...]]
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
			for k, v := range entry.Fields {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
			}
		}
	}

	conn.Write([]byte(resp.String()))
}

func handleXRead(conn io.Writer, commands []string) {
	if len(commands) < 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
		return
	}

	// Parse the command to check for BLOCK parameter
	blockTimeout := -1 // -1 means no blocking
	streamsIndex := 1

	// Check if BLOCK parameter is present
	if len(commands) > 2 && strings.ToLower(commands[1]) == "block" {
		if len(commands) < 5 {
			conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
			return
		}

		timeout, err := strconv.Atoi(commands[2])
		if err != nil || timeout < 0 {
			conn.Write([]byte("-ERR timeout is not an integer or out of range\r\n"))
			return
		}

		blockTimeout = timeout
		streamsIndex = 3
	}

	// Check for STREAMS keyword
	if strings.ToLower(commands[streamsIndex]) != "streams" {
		conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
		return
	}

	// Calculate number of streams
	remainingArgs := len(commands) - streamsIndex - 1
	if remainingArgs%2 != 0 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
		return
	}

	numStreams := remainingArgs / 2
	if numStreams == 0 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
		return
	}

	var streamKeys []string
	var lastIDs []string

	streamsStart := streamsIndex + 1
	for i := 0; i < numStreams; i++ {
		streamKeys = append(streamKeys, commands[streamsStart+i])
		lastIDs = append(lastIDs, commands[streamsStart+numStreams+i])
	}

	//expand $ into the most recent ID to only allow new entries
	for i, lid := range lastIDs {
		if lid == "$" {
			streamsMu.Lock()
			list := streams[streamKeys[i]]
			streamsMu.Unlock()

			if len(list) > 0 {
				// set to the current last entry
				lastIDs[i] = list[len(list)-1].ID
			} else {
				// empty stream ⇒ start from "0-0"
				lastIDs[i] = "0-0"
			}
		}
	}

	if blockTimeout >= 0 {
		handleBlockingXRead(conn, streamKeys, lastIDs, blockTimeout)
	} else {
		handleNonBlockingXRead(conn, streamKeys, lastIDs)
	}
}
