package dfs

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
)

type Client struct {
	nameServerAddr   string
	dataServerAddrs  []string
	dataServerClient []*rpc.Client
	nameServerClient *rpc.Client
}

func NewClient(nameServerAddr string, dataServerAddrs []string) *Client {
	return &Client{
		nameServerAddr:  nameServerAddr,
		dataServerAddrs: dataServerAddrs,
	}
}

func (c *Client) Connect() {

	client_logger.Println("Build Connection With Name Server:", c.nameServerAddr)
	dialHTTP, err := rpc.DialHTTP("tcp", c.nameServerAddr)
	if err != nil {
		client_logger.Println("Connect to Name Server failed")
		return
	}
	c.nameServerClient = dialHTTP

	for i, addr := range c.dataServerAddrs {
		client_logger.Printf("Build Connection With Data Server %d: %s\n", i, addr)
		dialHTTP, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			client_logger.Printf("Connect to Data Server %d failed", i)
			return
		}
		c.dataServerClient = append(c.dataServerClient, dialHTTP)
	}
	return
}

func (c *Client) Close() {
	c.nameServerClient.Close()
	for _, clt := range c.dataServerClient {
		clt.Close()
	}
}

func (c *Client) Download(filename string, dst string) {
	// 获取元数据
	req := FileDownloadMetaRequest{
		FileName: filename,
	}
	resp := &FileDownloadMetaResponse{
		DataServerId: make([]int, 0),
		ChunkId:      make([]int64, 0),
	}
	c.nameServerClient.Call("NameServer.Download", req, resp)
	fmt.Println(resp)
	// 根据文件元数据，下载
	newFilepath := filepath.Join(dst, filename)
	f, err := os.Create(newFilepath)
	if err != nil {
		ns_logger.Println("create file failed", err)
		return
	}
	for i, chunkId := range resp.ChunkId {
		chunkReadRequest := ChunkReadRequest{
			ChunkId: chunkId,
		}
		chunkReadResponse := &ChunkReadResponse{}
		c.dataServerClient[resp.DataServerId[i]].Call("DataServer.Download", chunkReadRequest, chunkReadResponse)
		f.Write(chunkReadResponse.DATA)
	}
	return
}

func (c *Client) Upload(file string) error {

	_, fileName := filepath.Split(file)
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		msg := fmt.Sprintf("file not exist %v\n", err)
		client_logger.Println(msg)
		return err
	}

	data := make([]byte, 2*1024*1024)
	var chunkID int64 = 0

	for {
		n, err := f.Read(data)
		if err != nil && err != io.EOF {
			client_logger.Printf("file read error %v\n", err)
			return err
		}
		if n == 0 {
			break
		}

		fileReq := FileUploadMetaRequest{
			FileName: fileName,
			ChunkId:  chunkID,
		}

		fileResp := &FileUploadMetaResponse{}
		err = c.nameServerClient.Call("NameServer.Upload", fileReq, fileResp)
		if err != nil {
			msg := fmt.Sprintf("meta data retreive fail %v\n", err)
			client_logger.Println(msg)
			return err
		}
		client_logger.Printf("file:%s, chunk:%d allocated to chunkId:%d dataserver:%d", fileName, fileReq.ChunkId, fileResp.ChunkId, fileResp.DataServerId)

		chunkReq := ChunkWriteRequest{
			ChunkId: fileResp.ChunkId,
			DATA:    data[:n],
		}
		chunkResp := &ChunkWriteResponse{}
		err = c.dataServerClient[fileResp.DataServerId].Call("DataServer.Upload", chunkReq, chunkResp)
		if err != nil {
			msg := fmt.Sprintf("upload file fail %v\n", err)
			client_logger.Println(msg)
			return err
		}
		chunkID += 1
	}
	return nil
}
