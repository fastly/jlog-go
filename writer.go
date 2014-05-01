package jlog

import (
	"reflect"
	"time"
	"unsafe"
)

/*
#cgo LDFLAGS: -ljlog
#include <jlog.h>
#include <sys/time.h>
*/
import "C"

type Writer struct {
	*Jlog
}

// NewWriter creates and returns a new jlog writer. If options is
// nil, the options default to SAFE, journal size 1kB, a non exclusive
// file and permissions of 0640. This function will initialize the jlog
// if it does not already exist. This call does not modify the passed in options.
func NewWriter(path string, options *Options) (Writer, error) {
	writer, e := newJlog(path, options)
	return Writer{writer}, e
}

func (log Writer) Open() error {
	return assertGTEZero(C.jlog_ctx_open_writer(log.ctx), "Open", log.Jlog)
}

func (log Writer) SendMessage(message []byte) (int, error) {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&message))
	data := unsafe.Pointer(header.Data)
	err := assertGTEZero(C.jlog_ctx_write(log.ctx, data, C.size_t(len(message))), "Write", log.Jlog)
	return len(message), err
}

func (log Writer) DateMessage(message []byte, when time.Time) (int, error) {
	var tv C.struct_timeval
	duration := when.Sub(time.Now())
	tv.tv_sec = float64ToTimeT(duration.Seconds())
	tv.tv_usec = int64ToSusecondsT(duration.Nanoseconds() / 1000)

	header := (*reflect.SliceHeader)(unsafe.Pointer(&message))
	data := unsafe.Pointer(header.Data)

	var msg C.jlog_message
	msg.mess_len = C.u_int32_t(len(message))
	msg.mess = data
	// The header fields are left uninitialized because they are not used
	// anywhere down the stracktrace of writing a message (only mess and mess_len
	// are used. Additionally, the header values are lower level metadata
	// information about timing and length. The length is already visible
	// in the []byte length, the timing of when a message is read is seems
	// less important.

	bytesWritten := C.jlog_ctx_write_message(log.ctx, &msg, &tv)

	return int(bytesWritten), assertGTEZero(bytesWritten, "WriteMessage", log.Jlog)
}
