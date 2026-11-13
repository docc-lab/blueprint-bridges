package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/UniqueIdService/UniqueIdHandler.h. (The Blueprint port kept one counter digit, the FIRST one, so its
// ids repeated above 16 posts per millisecond; it also used no epoch and no lock.)

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"
)

// The UniqueIdService interface
type UniqueIdService interface {
	// Returns a newly generated unique id to be used as a post's unique identifier.
	ComposeUniqueId(ctx context.Context, reqID int64, postType int64) (int64, error)
}

// Implementation of [UniqueIdService]
// Tomislav-RetCtx: upstream keeps current_timestamp and counter as statics (lines 25-26) and the machine id and a
// mutex in the handler (lines 51-53).
type UniqueIdServiceImpl struct {
	counter           int64
	current_timestamp int64
	machine_id        string
	lock              sync.Mutex
}

// Tomislav-RetCtx: CUSTOM_EPOCH, line 17 (2018-01-01T00:00:00Z in ms).
const customEpoch = 1514764800000

// Implements UniqueIdService interface
// Tomislav-RetCtx: UniqueIdHandler::UniqueIdHandler, lines 55-59 (the machine id comes from GetMachineId, see common.go).
func NewUniqueIdServiceImpl(ctx context.Context) (UniqueIdService, error) {
	return &UniqueIdServiceImpl{counter: 0, current_timestamp: -1, machine_id: GetMachineID()}, nil
}

// Tomislav-RetCtx: GetCounter, lines 28-40: the counter restarts at 0 every millisecond; a timestamp older than the last
// one is fatal ("Timestamps are not incremental.", exit).
func (u *UniqueIdServiceImpl) getCounter(timestamp int64) int64 {
	if u.current_timestamp > timestamp { // lines 29-32
		log.Fatalf("Timestamps are not incremental.")
	}
	if u.current_timestamp == timestamp { // lines 33-34
		retVal := u.counter
		u.counter += 1
		return retVal
	}
	u.current_timestamp = timestamp // lines 35-38
	u.counter = 1
	return 0
}

// Implements UniqueIdService interface
// Tomislav-RetCtx: UniqueIdHandler::ComposeUniqueId, lines 61-109.
func (u *UniqueIdServiceImpl) ComposeUniqueId(ctx context.Context, reqID int64, postType int64) (int64, error) {
	u.lock.Lock()                                     // line 73
	timestamp := time.Now().UnixMilli() - customEpoch // lines 74-77
	idx := u.getCounter(timestamp)                    // line 78
	u.lock.Unlock()                                   // line 79
	// Tomislav-RetCtx: lines 81-103: machine id + last 10 hex digits of the timestamp + last 3 hex digits of the counter.
	postIDStr := u.machine_id + lastHex(timestamp, 10) + lastHex(idx, 3)
	postID, err := strconv.ParseUint(postIDStr, 16, 64) // line 104 (stoul, then the sign bit cleared)
	if err != nil {
		return 0, err
	}
	return int64(postID & 0x7FFFFFFFFFFFFFFF), nil // lines 104 (sign bit cleared), 108
}
