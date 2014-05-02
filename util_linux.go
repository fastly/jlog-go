package jlog

/*
#include <sys/time.h>
*/
import "C"

func float64ToTimeT(num float64) C.__time_t {
	return C.__time_t(num)
}

func int64ToSusecondsT(num int64) C.__suseconds_t {
	return C.__suseconds_t(num)
}
