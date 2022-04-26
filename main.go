package main

import "mini-dfs/dfs"

func initDataServer(addrs []string) {
	for i, addr := range addrs {
		server := dfs.NewDataServer(addr, i)
		go server.RunRpcServer()
	}
	return
}

func main() {

	nameServerAddr := "127.0.0.1:8080"

	dataServerAddr := []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084"}
	initDataServer(dataServerAddr)

	client := dfs.NewClient(nameServerAddr, dataServerAddr)
	client.Connect()
	client.Upload("./dfs/tmp/p15181070171.jpg")
	client.Close()
}
