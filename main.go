package main

import (
	"flag"
	"mini-dfs/db"
	"mini-dfs/dfs"
)

func main() {
	var appName = flag.String("app", "nn", "AppName")
	var namenodeAddr = flag.String("nn-addr", "127.0.0.1:8080", "NameNode Addr")
	var datanodeAddr = flag.String("dn-addr", "127.0.0.1:8081", "DataNode Addr")
	flag.Parse()
	db.InitDB()
	if *appName == "nn" || *appName == "namenode" {
		nameNode := dfs.NewNameServer(*namenodeAddr)
		nameNode.Run()
	} else if *appName == "dn" || *appName == "datanode" {
		dataNode := dfs.NewDataNode(*datanodeAddr, *namenodeAddr)
		dataNode.Run()
	} else {
		client := dfs.NewClient(*namenodeAddr)
		client.Connect()
		client.UploadFile("./data/tmp/2.pdf")
		client.Download("2.pdf", "./data/test")
		client.UploadFile("./data/tmp/4.pdf")
		client.Download("4.pdf", "./data/test")
		client.Close()
	}

}
