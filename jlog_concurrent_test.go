package jlog_test

import (
	"github.com/fastly/jlog-go"
	"log"
	"os"
	"runtime"
	"strconv"
	"testing"
)

func TestConcurrent(t *testing.T) {
	initialize(t)
	runtime.GOMAXPROCS(8)
	e := os.Mkdir("./concurrent", 0777)
	if e != nil {
		t.Errorf("unable to make temporary directory")
		return
	}
	subscribers := []string{
		"one",
		"two",
		"three",
		"four",
		"five",
		"six",
		"seven",
		"eight",
		"nine",
	}
	sendDoneChans := make([]chan struct{}, len(subscribers))
	readDoneChans := make([]chan struct{}, len(subscribers))
	for i := range sendDoneChans {
		sendDoneChans[i] = make(chan struct{}, 1)
		readDoneChans[i] = make(chan struct{}, 1)
	}

	writeCount := 10000

	writer, _ := jlog.NewWriter(pathname, nil)
	for i, v := range subscribers {
		writer.AddSubscriber(v, jlog.BEGIN)

		f, e := os.Create("./concurrent/" + v)
		if e != nil {
			t.Errorf("unable to open file")
			os.Exit(1)
		}

		reader, e := jlog.NewReader(pathname, nil)
		if e != nil {
			t.Errorf("unable to new reader")
			return
		}
		e = reader.Open(v)
		if e != nil {
			log.Printf("unable to open reader, error %v, errorString %v", e, reader.ErrString())
			os.Exit(1)
		}

		go concurrentReading(reader, f, sendDoneChans[i], readDoneChans[i], t)
	}
	e = writer.Open()
	if e != nil {
		t.Errorf("cannot open for writing, %v", writer.ErrString())
	}
	for i := 0; i < writeCount; i++ {
		writer.Write([]byte(strconv.Itoa(i)))
	}
	log.Printf("done writing for concurrent test")
	for _, c := range sendDoneChans {
		c <- struct{}{}
	}
	for _, c := range readDoneChans {
		<-c
	}
	e = os.Rename("./concurrent", "./concurrentOld")
	if e != nil {
		log.Printf("unable to rename new concurrent folder to concurrentOld, error %v, left name as is", e.Error())
	}
}

func concurrentReading(reader jlog.Reader, f *os.File, sendDoneChan <-chan struct{}, readDoneChan chan<- struct{}, t *testing.T) {
out:
	for {
		select {
		case <-sendDoneChan:
			break out
		default:
			bytes, e := reader.GetMessage()
			if e != nil {
				break out
			}
			if len(bytes) > 0 {
				_, e = f.Write(bytes)
				if e != nil {
					t.Errorf("file write error %s", e.Error())
					break out
				}
			}
		}
	}
	for {
		bytes, e := reader.GetMessage()
		if bytes == nil || e != nil {
			break
		}
		if len(bytes) > 0 {
			_, e = f.Write(bytes)
			if e != nil {
				t.Errorf("file write error %s", e.Error())
				break
			}
		}
	}
	f.Close()
	readDoneChan <- struct{}{}

}
