package dfs

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
)

type DataServer struct {
	addr      string
	rpcServer *rpc.Server
	id        int
}

func NewDataServer(addr string, id int) *DataServer {
	return &DataServer{
		addr: addr,
		id:   id,
	}
}

func (d *DataServer) RunRpcServer() (net.Listener, error) {

	ds_logger.Printf("Run data server: %s\n", d.addr)
	server := rpc.NewServer()
	server.Register(d)
	d.rpcServer = server
	listener, err := net.Listen("tcp", d.addr)
	if err != nil {
		ds_logger.Println("Listen error", err)
		return nil, err
	}
	return listener, http.Serve(listener, server)
}

func (d *DataServer) Upload(req ChunkWriteRequest, res *ChunkWriteResponse) error {
	newPath := filepath.Join("./data/ds"+strconv.Itoa(d.id), "chunk"+strconv.FormatInt(req.ChunkId, 10))
	f, err := os.Create(newPath)
	defer f.Close()
	if err != nil {
		msg := fmt.Sprintf("Create file failed: %s\n", newPath)
		ds_logger.Println(msg)
		res.msg = msg
		return err
	}
	_, err = f.Write(req.DATA)
	if err != nil {
		msg := fmt.Sprintf("Write file failed: %s\n", newPath)
		ds_logger.Println(msg)
		res.msg = msg
		return err
	}
	return nil
}
