package dfs

import (
	"crypto/md5"
	"io"
	"log"
	"os"
)

//Logger1.AddFilter("file", logger.DEBUG, logger.NewFileLogWriter(logger1File, rotateStatus))
var (
	ns_logger     *log.Logger
	ds_logger     *log.Logger
	client_logger *log.Logger
)

func init() {
	file, err := os.OpenFile("./log/default.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Faild to open error logger file:", err)
	}
	log.SetOutput(io.Writer(file))

	file, err = os.OpenFile("./log/ns.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Faild to open error logger file:", err)
	}
	ns_logger = log.New(io.MultiWriter(file), "NameNode:", log.Lshortfile)
	ns_logger.Println("======================================================")

	file, err = os.OpenFile("./log/ds.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Faild to open error logger file:", err)
	}
	ds_logger = log.New(io.MultiWriter(file), "DataNode:", log.Lshortfile)
	ds_logger.Println("======================================================")

	file, err = os.OpenFile("./log/client.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Faild to open error logger file:", err)
	}
	client_logger = log.New(io.MultiWriter(file), "client:", log.Lshortfile)
	client_logger.Println("======================================================")
}

func MD5Encode(chunk []byte) []byte {
	h := md5.New()
	h.Write(chunk)
	return h.Sum(nil)
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}
