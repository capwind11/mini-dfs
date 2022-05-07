package dfs

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
)

type Client struct {
	nameNodeAddr   string
	dataNodeAddrs  []string
	dataNodeClient map[string]*rpc.Client
	nameNodeClient *rpc.Client
}

func NewClient(nameNodeAddr string) *Client {
	return &Client{
		nameNodeAddr:   nameNodeAddr,
		dataNodeClient: make(map[string]*rpc.Client),
	}
}

func (c *Client) Connect() {

	client_logger.Println("Build Connection With Name Server:", c.nameNodeAddr)
	dialHTTP, err := rpc.DialHTTP("tcp", c.nameNodeAddr)
	if err != nil {
		client_logger.Println("Connect to Name Server failed")
		return
	}
	c.nameNodeClient = dialHTTP
	dataNodeInfoReq := DataNodeInfoReq{}
	dataNodeInfoResp := &DataNodeInfoResp{}
	err = c.nameNodeClient.Call("NameNode.GetDataNodeAddrs", dataNodeInfoReq, dataNodeInfoResp)
	if err != nil {
		client_logger.Println("get datanode info failed")
		return
	}
	for i, addr := range dataNodeInfoResp.Addrs {
		c.dataNodeAddrs = append(c.dataNodeAddrs, addr)
		client_logger.Printf("Build Connection With DataNode %d: %s\n", i, addr)
		dialHTTP, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			client_logger.Printf("Connect to DataNode %d failed", i)
			return
		}
		c.dataNodeClient[addr] = dialHTTP
	}
	return
}

func (c *Client) Close() {
	c.nameNodeClient.Close()
}

func (c *Client) UploadFile(file string) error {
	_, fileName := filepath.Split(file)
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		client_logger.Printf("file not exist %v\n", err)
		return err
	}
	stat, _ := f.Stat()
	size := stat.Size()
	fileUploadMetaRequest := FileUploadMetaRequest{
		FileName: fileName,
		FileSize: size,
	}
	fileUploadMetaResponse := &FileUploadMetaResponse{}
	err = c.nameNodeClient.Call("NameNode.Upload", fileUploadMetaRequest, fileUploadMetaResponse)
	if err != nil {
		client_logger.Printf("file metadata init failed %v\n", err)
		return err
	}
	data := make([]byte, CHUNK_SIZE)
	for _, chunk := range fileUploadMetaResponse.ChunkInfo {
		n, err := f.Read(data)
		if err != nil && err != io.EOF {
			client_logger.Printf("file read error %v\n", err)
			return err
		}

		chunkReq := ChunkWriteRequest{
			ChunkId:   chunk.ChunkId,
			DATA:      data[:n],
			DataNodes: chunk.DataNodeAddrs,
			MD5Code:   MD5Encode(data[:n]),
		}
		chunkResp := &ChunkWriteResponse{}
		err = c.dataNodeClient[chunk.DataNodeAddrs[0]].Call("DataNode.Upload", chunkReq, chunkResp)
		if err != nil {
			msg := fmt.Sprintf("upload chunk %d fail %v\n", chunk.ChunkId, err)
			client_logger.Println(msg)
			return err
		}
	}
	return err
}

func (c *Client) Download(filename string, dst string) {
	// 获取元数据
	req := FileDownloadMetaRequest{
		FileName: filename,
	}
	resp := &FileDownloadMetaResponse{
		DataServerAddrs: make([]string, 0),
		ChunkId:         make([]int64, 0),
	}
	c.nameNodeClient.Call("NameNode.Download", req, resp)
	//fmt.Println(resp)
	// 根据文件元数据，下载
	newFilepath := filepath.Join(dst, filename)
	f, err := os.Create(newFilepath)
	defer f.Close()
	if err != nil {
		ns_logger.Println("create file failed", err)
		return
	}
	for i, chunkId := range resp.ChunkId {
		chunkReadRequest := ChunkReadRequest{
			ChunkId: chunkId,
		}
		chunkReadResponse := &ChunkReadResponse{}
		c.dataNodeClient[resp.DataServerAddrs[i]].Call("DataNode.Download", chunkReadRequest, chunkReadResponse)
		decodeString, _ := hex.DecodeString(resp.MD5Code[i])
		if !bytes.Equal(MD5Encode(chunkReadResponse.DATA), decodeString) {
			ns_logger.Println("file checked failed", err)
			return
		}
		f.Write(chunkReadResponse.DATA)
	}
	return
}
