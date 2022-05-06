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
	//for _, s := range ds {
	//	s.Connect()
	//}
	return
}

func main() {
	db.InitDB()
	nameServerAddr := "127.0.0.1:8080"
	nameServer := dfs.NewNameServer(nameServerAddr)
	go nameServer.RunRpcServer()

	dataServerAddr := []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084"}
	initDataServer(dataServerAddr)

	client := dfs.NewClient(nameServerAddr, dataServerAddr)
	client.Connect()
	client.Upload("./data/tmp/2.pdf")
	client.Download("2.pdf", "./data/test")
	client.Close()
}
