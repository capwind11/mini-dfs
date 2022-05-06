package dfs

import (
	"fmt"
	"mini-dfs/db"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
)

type DataServer struct {
	id        int           // 当前服务器ID
	addr      string        // 当前DataServer地址
	rpcServer *rpc.Server   // RPC服务器
	peerAddrs []string      // 其它DataServer地址
	peers     []*rpc.Client // 其它DataServer的RPC服务器
}

func NewDataServer(addr string, id int, addrs []string) *DataServer {
	return &DataServer{
		addr:      addr,
		id:        id,
		peerAddrs: addrs,
	}
}

//
//func (d *DataServer) Connect() {
//
//	for j := 0; j < 4; j += 1 {
//		dialHTTP, err := rpc.DialHTTP("tcp", d.peerAddrs[j])
//		if err != nil {
//			ds_logger.Printf("DataServer %d Connect to Data Server %d failed", d.id, j)
//			return
//		}
//		d.peers = append(d.peers, dialHTTP)
//	}
//}

func (d *DataServer) Close() {
	for j := 0; j < 4; j += 1 {
		d.peers[j].Close()
	}
}

func (d *DataServer) Call(peerId int, method string, req interface{}, resp interface{}) (err error) {
	addr := d.peerAddrs[peerId]
	var client *rpc.Client
	if client, err = rpc.DialHTTP("tcp", addr); err != nil {
		return err
	}
	defer client.Close()
	return client.Call(method, req, resp)
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

func (d *DataServer) Write(req ChunkWriteRequest, res *ChunkWriteResponse) error {
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

func (d *DataServer) Upload(req ChunkWriteRequest, res *ChunkWriteResponse) error {
	md5Encode := MD5Encode(req.DATA)
	//fmt.Println(hex.EncodeToString(md5Encode))
	db.UpdateChunk(req.ChunkId, md5Encode)
	err := d.Write(req, res)
	if err != nil {
		msg := fmt.Sprintf("Write file failed\n")
		ds_logger.Println(msg)
		res.msg = msg
		return err
	}
	for i := 1; i < 3; i += 1 {
		target := (d.id + i) % 4
		chunkResp := ChunkWriteResponse{}
		er := d.Call(target, "DataServer.Write", req, chunkResp)
		ds_logger.Printf("Chunk: %d is transferred to dataserver: %d\n", req.ChunkId, target)
		if er != nil {
			ds_logger.Println("transferred failed", er)
		}
	}
	return nil
}

func (d *DataServer) PeerUpload(req ChunkWriteRequest, res *ChunkWriteResponse) error {
	return d.Write(req, res)
}

func (d *DataServer) Download(req ChunkReadRequest, res *ChunkReadResponse) error {
	newPath := filepath.Join("./data/ds"+strconv.Itoa(d.id), "chunk"+strconv.FormatInt(req.ChunkId, 10))
	f, err := os.Open(newPath)
	defer f.Close()
	if err != nil {
		msg := fmt.Sprintf("Open file failed: %s\n", newPath)
		ds_logger.Println(msg)
		res.msg = msg
		return err
	}
	res.DATA = make([]byte, 2*1024*1024)

	n, err := f.Read(res.DATA)
	if err != nil {
		msg := fmt.Sprintf("Write file failed: %s\n", newPath)
		ds_logger.Println(msg)
		res.msg = msg
		return err
	}
	res.DATA = res.DATA[:n]
	res.MD5Code = MD5Encode(res.DATA)
	return nil
}
