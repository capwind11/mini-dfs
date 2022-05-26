package main

import (
	"flag"
	"mini-dfs/db"
	"mini-dfs/dfs"
)

func main() {
	// 进程类型，nn代表namenode，dn代表datanode，否则为客户端client
	var appName = flag.String("app", "nn", "AppName")
	// 获取namenode绑定的地址
	var nameNodeAddr = flag.String("nn-addr", "127.0.0.1:8080", "NameNode Addr")
	// 获取datanode绑定的地址
	var dataNodeAddr = flag.String("dn-addr", "127.0.0.1:8081", "DataNode Addr")
	flag.Parse()

	// 执行不同类别进程
	if *appName == "nn" || *appName == "namenode" {
		nameNode := dfs.NewNameServer(*nameNodeAddr)
		db.InitDB()
		nameNode.Run()
	} else if *appName == "dn" || *appName == "datanode" {
		dataNode := dfs.NewDataNode(*dataNodeAddr, *nameNodeAddr)
		db.InitDB()
		dataNode.Run()
	} else {
		client := dfs.NewClient(*nameNodeAddr)
		client.Run()
	}
}
