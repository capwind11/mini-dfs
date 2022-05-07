package dfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mini-dfs/db"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
)

type DataNode struct {
	id           int    // 当前服务器ID
	addr         string // 当前DataServer地址
	nameNodeAddr string
	rpcServer    *rpc.Server // RPC服务器
	dataPath     string
	listener     net.Listener
}

func NewDataNode(addr string, nameServerAddr string) *DataNode {

	return &DataNode{
		addr:         addr,
		nameNodeAddr: nameServerAddr,
	}
}

func (d *DataNode) Run() {
	ds_logger.Printf("Run DataNode: %s\n", d.addr)
	server := rpc.NewServer()

	server.Register(d)
	d.rpcServer = server
	listener, err := net.Listen("tcp", d.addr)
	d.listener = listener
	if err != nil {
		ds_logger.Println("Listen error:", err)
	}
	go http.Serve(listener, server)

	d.ConnectToNameNode()
	c := make(chan os.Signal)
	_ = <-c

}

func (d *DataNode) ConnectToNameNode() error {
	req := DataNodeConnectRequest{
		Addr: d.addr,
	}
	resp := &DataNodeConnectResponse{}
	err := d.Call(d.nameNodeAddr, "NameNode.ConnectToNameNode", req, resp)
	if err != nil || resp.STATUS == FAILED {
		ds_logger.Println("Connect to NameNode failed")
		return errors.New("connect to NameNode failed")
	}
	d.id = resp.Id

	d.dataPath = DATA_PATH + strconv.Itoa(d.id)
	_, err = os.Stat(d.dataPath)
	if err != nil {
		os.Mkdir(d.dataPath, 755)
	}
	ds_logger.Printf("DataNode %d connect to NameNode success\n", d.id)
	return nil
}

func (d *DataNode) Call(addr string, method string, req interface{}, resp interface{}) (err error) {

	var client *rpc.Client
	if client, err = rpc.DialHTTP("tcp", addr); err != nil {
		return err
	}
	defer client.Close()
	return client.Call(method, req, resp)
}

func (d *DataNode) RunRpcServer() (net.Listener, error) {

	ds_logger.Printf("Run DataNode: %s\n", d.addr)
	server := rpc.NewServer()

	server.Register(d)
	d.rpcServer = server
	listener, err := net.Listen("tcp", d.addr)
	d.listener = listener
	if err != nil {
		ds_logger.Println("Listen error:", err)
		return nil, err
	}
	return listener, http.Serve(listener, server)
}

func (d *DataNode) Close() {

}

func (d *DataNode) Write(req ChunkWriteRequest, res *ChunkWriteResponse) error {

	newPath := filepath.Join(d.dataPath, "chunk"+strconv.FormatInt(req.ChunkId, 10))
	db.InsertChunk2Node(req.ChunkId, d.addr)
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

func (d *DataNode) Upload(req ChunkWriteRequest, res *ChunkWriteResponse) error {

	md5Encode := MD5Encode(req.DATA)
	if !bytes.Equal(md5Encode, req.MD5Code) {
		ds_logger.Printf("check chunk %d md5 fail", req.ChunkId)
		return errors.New("check chunk md5 fail")
	}
	db.UpdateChunk(req.ChunkId, md5Encode)
	err := d.Write(req, res)
	if err != nil {
		msg := fmt.Sprintf("Write file failed\n")
		ds_logger.Println(msg)
		res.msg = msg
		return err
	}
	for i := 1; i < len(req.DataNodes); i += 1 {
		target := req.DataNodes[i]
		chunkResp := &ChunkWriteResponse{}
		er := d.Call(target, "DataNode.Write", req, chunkResp)
		ds_logger.Printf("Chunk: %d is transferred to dataserver: %s\n", req.ChunkId, target)
		if er != nil {
			ds_logger.Println("transferred failed", er)
		}
	}
	return nil
}

func (d *DataNode) Download(req ChunkReadRequest, res *ChunkReadResponse) error {
	newPath := filepath.Join(d.dataPath, "chunk"+strconv.FormatInt(req.ChunkId, 10))
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

func (d *DataNode) HeartBeat(req HeartBeatRequest, res *HeartBeatResponse) error {
	res.STATUS = SUCCESS
	return nil
}

func (d *DataNode) PeerReplicate(req PeerReplicateRequest, res *PeerReplicateResponse) error {
	data := make([]byte, CHUNK_SIZE)
	for i, chunkId := range req.ChunkId {
		f, _ := os.Open(filepath.Join(d.dataPath, "chunk"+strconv.FormatInt(chunkId, 10)))
		n, err := f.Read(data)
		if err != nil && err != io.EOF {
			client_logger.Printf("file read error %v\n", err)
			res.Status = FAILED
			return err
		}
		target := req.DataServerAddrs[i]
		chunkResp := &ChunkWriteResponse{}
		chunkReq := ChunkWriteRequest{
			ChunkId: chunkId,
			DATA:    data[:n],
			MD5Code: MD5Encode(data[:n]),
		}
		d.Call(target, "DataNode.Write", chunkReq, chunkResp)
	}
	res.Status = SUCCESS
	return nil
}
