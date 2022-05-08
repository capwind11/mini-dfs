package main

import (
	"mini-dfs/db"
	"mini-dfs/dfs"
	"testing"
)

func initDataServer(addrs []string, nameServerAddr string) []*dfs.DataNode {
	ds := make([]*dfs.DataNode, 0)
	for _, addr := range addrs {
		server := dfs.NewDataNode(addr, nameServerAddr)
		ds = append(ds, server)
		go server.RunRpcServer()
		server.ConnectToNameNode()
	}
	return ds
}

func TestUploadDownload(t *testing.T) {
	db.InitDB()
	dfs.REPLICATE_NUM = 2

	dataServerAddr := []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084"}
	nameServerAddr := "127.0.0.1:8080"
	nameServer := dfs.NewNameServer(nameServerAddr)
	go nameServer.RunRpcServer()
	go nameServer.RunServer()
	initDataServer(dataServerAddr, nameServerAddr)
	client := dfs.NewClient(nameServerAddr)
	client.Connect()
	client.UploadFile("./data/tmp/2.pdf")
	client.Download("2.pdf", "./data/test")
	client.UploadFile("./data/tmp/4.pdf")
	client.Download("4.pdf", "./data/test")

	//nameServer.DataRecovery("127.0.0.1:8082")
	client.Close()
}
