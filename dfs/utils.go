package dfs

import (
	"crypto/md5"
	"log"
	"os"
)

var ns_logger = log.New(os.Stdout, "NameServer:", log.Lshortfile)
var ds_logger = log.New(os.Stdout, "DataServer:", log.Lshortfile)
var client_logger = log.New(os.Stdout, "client:", log.Lshortfile)

func MD5Encode(chunk []byte) []byte {
	h := md5.New()
	h.Write(chunk)
	return h.Sum(nil)
}

func Show() {

}
