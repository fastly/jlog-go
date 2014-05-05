package jlog

/*
#include <sys/time.h>
*/
import "C"

func float64ToTimeT(num float64) C.__darwin_time_t {
	return C.__darwin_time_t(num)
}

func int64ToSusecondsT(num int64) C.__darwin_suseconds_t {
	return C.__darwin_suseconds_t(num)
}
