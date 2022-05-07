package dfs

import "time"

var CHUNK_SIZE int64 = 2 * 1024 * 1024

var DATA_PATH = "./data/ds"

var HeartBeatIntervalTime = 200 * time.Millisecond

const (
	SUCCESS int = 1
	FAILED  int = -1
	UNKNOWN int = 0
)
