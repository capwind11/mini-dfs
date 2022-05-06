package main

import (
	"mini-dfs/db"
	"mini-dfs/dfs"
)

func initDataServer(addrs []string) {
	ds := make([]*dfs.DataServer, 0)
	for i, addr := range addrs {
		server := dfs.NewDataServer(addr, i, addrs)
		ds = append(ds, server)
		go server.RunRpcServer()
	}
	return
}

func main() {
	db.InitDB()

	dataServerAddr := []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084"}
	nameServerAddr := "127.0.0.1:8080"
	nameServer := dfs.NewNameServer(nameServerAddr, dataServerAddr)
	go nameServer.RunRpcServer()
	initDataServer(dataServerAddr)

	client := dfs.NewClient(nameServerAddr, dataServerAddr)
	client.Connect()
	//client.UploadFile("./data/tmp/2.pdf")
	client.Download("2.pdf", "./data/test")
	//client.UploadFile("./data/tmp/4.pdf")
	client.Download("4.pdf", "./data/test")
	client.Close()
}
