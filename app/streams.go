package main

import (
	"strconv"
	"strings"
	"time"
)

// generateSequenceNum generates the next sequence number for a stream entry
func generateSequenceNum(idPtr *string, key string) {
	streamList := streams[key]
	maxSeq := -1
	timePart := strings.Split(*idPtr, "-")[0]
	for _, entry := range streamList {
		parts := strings.SplitN(entry.ID, "-", 2)
		if len(parts) != 2 || parts[0] != timePart {
			continue
		}
		if seq, err := strconv.Atoi(parts[1]); err == nil && seq > maxSeq {
			maxSeq = seq
		}
	}
	if timePart == strconv.Itoa(0) && maxSeq == -1 {
		*idPtr = timePart + "-" + strconv.Itoa(1)
	} else {
		*idPtr = timePart + "-" + strconv.Itoa(maxSeq+1)
	}
}

// generateEntryID generates a new entry ID for a stream
func generateEntryID(idPtr *string, key string) {
	timePart := strconv.FormatInt(time.Now().UnixMilli(), 10)
	*idPtr = timePart + "-" + "*"
	generateSequenceNum(idPtr, key)
}

// getStreamNotifier gets or creates a notification channel for a stream
func getStreamNotifier(streamKey string) chan struct{} {
	streamNotifiersMu.Lock()
	defer streamNotifiersMu.Unlock()

	if notifier, exists := streamNotifiers[streamKey]; exists {
		return notifier
	}

	notifier := make(chan struct{}, 1)
	streamNotifiers[streamKey] = notifier
	return notifier
}

// notifyWaitingXReads notifies waiting XREAD commands when new entries are added
func notifyWaitingXReads(streamKey string) {
	streamNotifiersMu.Lock()
	if notifier, exists := streamNotifiers[streamKey]; exists {
		select {
		case notifier <- struct{}{}:
		default:
		}
	}
	streamNotifiersMu.Unlock()
}

// hasNewEntries checks if there are new entries for a stream
func hasNewEntries(streamKey string, lastID string) bool {
	streamsMu.Lock()
	defer streamsMu.Unlock()

	streamList, exists := streams[streamKey]
	if !exists {
		return false
	}

	for _, entry := range streamList {
		if entry.ID > lastID {
			return true
		}
	}
	return false
}

// getNewEntries gets new entries for a stream
func getNewEntries(streamKey string, lastID string) []streamEntry {
	streamsMu.Lock()
	defer streamsMu.Unlock()

	streamList, exists := streams[streamKey]
	if !exists {
		return nil
	}

	var entries []streamEntry
	for _, entry := range streamList {
		if entry.ID > lastID {
			entries = append(entries, entry)
		}
	}
	return entries
}
