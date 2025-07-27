package main

import (
	"time"
)

// getValue retrieves a value from the database, handling expiration
func getValue(key string) (string, bool) {
	val, exists := db[key]
	expiry, hasExpiry := expiryDB[key]
	if hasExpiry && time.Now().After(expiry) {
		// Key expired, delete it
		delete(db, key)
		delete(expiryDB, key)
		exists = false // Mark as not existing after deletion
	}
	if !exists {
		return "", false
	}
	return val, exists
}

// propagateToReplicas sends commands to all connected replicas
func propagateToReplicas(commands []string) {
	msg := encodeRESPArray(commands...)
	replicaConnsMu.Lock()
	defer replicaConnsMu.Unlock()
	for c := range replicaConns {
		_, err := c.Write([]byte(msg))
		if err != nil {
			// Remove dead connections
			c.Close()
			delete(replicaConns, c)
		}
	}
}
