package main

import (
	"mini-dfs/db"
	"mini-dfs/dfs"
)

func main() {
	db.InitDB()
	dataServerAddr := []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084"}
	nameServerAddr := "127.0.0.1:8080"
	nameNode := dfs.NewNameServer(nameServerAddr)
	go nameNode.Run()
	for _, addr := range dataServerAddr {
		dataNode := dfs.NewDataNode(addr, nameServerAddr)
		go dataNode.RunRpcServer()
		dataNode.ConnectToNameNode()
	}
	client := dfs.NewClient(nameServerAddr)
	client.Run()
}
