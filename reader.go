package jlog

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/twmb/message"
)

/*
#cgo LDFLAGS: -ljlog
#include <jlog.h>
#include <stdlib.h>
*/
import "C"

type Reader struct {
	*Jlog
}

var zeroId C.jlog_id
var mutex sync.Mutex

// NewReader creates and returns a new jlog reader. If options is
// nil, the options default to SAFE, journal size 1kB, a non exclusive
// file and permissions of 0640. This function will initialize the jlog
// if it does not already exist. This call does not modify the passed in options.
func NewReader(path string, options *Options) (Reader, error) {
	reader, e := newJlog(path, options)
	return Reader{reader}, e
}

func (log Reader) Open(subscriber string) error {
	s := C.CString(subscriber)
	defer C.free(unsafe.Pointer(s))
	return assertGTEZero(C.jlog_ctx_open_reader(log.ctx, s), "Open", log.Jlog)
}

func (log Reader) NumAvailable() (int, error) {
	count := C.jlog_ctx_read_interval(log.ctx, &log.start, &log.end)
	return int(count), assertGTEZero(count, "NumAvailable", log.Jlog)
}

// GetMessage reads the next message from the JLog queue.
func (log Reader) GetMessage() ([]byte, error) {
	var currentId C.jlog_id
	var jmsg C.jlog_message

	/* if start is unset, we need to read the interval (again) */
	if log.readErrd || log.start == zeroId {
		log.readErrd = false
		count := C.jlog_ctx_read_interval(log.ctx, &log.start, &log.end)
		if count == 0 || count == -1 && log.Err() == ERR_FILE_OPEN {
			log.start = zeroId
			log.end = zeroId
			return nil, message.EOMs
		}
		if count == -1 {
			return nil, assertGTEZero(count, "Read", log.Jlog)
		}
	}
	/* if last is unset, start at the beginning */
	if log.last == zeroId {
		currentId = log.start
	} else {
		/* if we've already read the end, return; otherwise advance */
		currentId = log.last
		if log.prev == log.end {
			log.start = zeroId
			log.end = zeroId
			return nil, message.EOMs
		}
		C.jlog_ctx_advance_id(log.ctx, &log.last, &currentId, &log.end)
		if log.last == currentId {
			log.start = zeroId
			log.end = zeroId
			return nil, message.EOMs
		}
	}
	mutex.Lock()
	e := C.jlog_ctx_read_message(log.ctx, &currentId, &jmsg)
	if e != 0 {
		log.readErrd = true
		mutex.Unlock()
		return nil, assertGTEZero(e, "Read", log.Jlog)
	}
	var s []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	header.Data = uintptr(jmsg.mess)
	header.Len = int(jmsg.mess_len)
	header.Cap = int(jmsg.mess_len)
	copied := make([]byte, len(s))
	copy(copied, s)
	mutex.Unlock()
	if log.autoCheckpt {
		e := C.jlog_ctx_read_checkpoint(log.ctx, &currentId)
		if e != 0 {
			return nil, assertGTEZero(e, "Read autocheckpoint", log.Jlog)
		}
		log.last = zeroId
		log.prev = zeroId
		log.start = zeroId
		log.end = zeroId
	} else {
		log.prev = log.last
		log.last = currentId
	}
	if len(copied) == 0 {
		return nil, message.EOMs
	}
	return copied, nil
}

func (log Reader) AckMsgGot() error {
	return log.Checkpoint()
}

// Rewind rewinds the jlog to the previous transaction id (when in an uncommitted state).
// This is useful for implementing a 'peek' style action.
func (log Reader) Rewind() {
	log.last = log.prev
}

// Checkpoint checkpoints your read. This will notify the JLog that you have successfully
// read logs up to this point. If all registered subscribers have read to
// a certain point, the JLog system can remove the underlying data for the
// read messages.
func (log Reader) Checkpoint() error {
	if log.last != zeroId {
		e := C.jlog_ctx_read_checkpoint(log.ctx, &log.last)
		if e < 0 {
			return assertGTEZero(e, "Checkpoint", log.Jlog)
		}
		// we have to re-read the interval after a checkpoint
		log.last = zeroId
		log.start = zeroId
		log.end = zeroId
	}
	return nil
}

// AutoCheckpoint returns the auto checkpoint property. If enabled is not nil,
// this function will set the auto checkpoint property to the value. With
// auto-checkpointing enabled, the reader will automatically
// checkpoint whenever you call read().
func (log Reader) AutoCheckpoint(enabled *bool) bool {
	if enabled != nil {
		log.autoCheckpt = *enabled
	}
	return log.autoCheckpt
}
