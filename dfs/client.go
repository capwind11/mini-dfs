package dfs

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
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

func (c *Client) Download() {

}

func (c *Client) Upload(filePath string) error {

	f, err := os.Open(filePath)
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
		req := ChunkWriteRequest{
			ChunkId: chunkID,
			DATA:    data[:n],
		}
		res := &ChunkWriteResponse{}
		err = c.dataServerClient.Call("DataServer.Upload", req, res)
		if err != nil {
			msg := fmt.Sprintf("upload file not fail %v\n", err)
			client_logger.Println(msg)
			return err
		}
		chunkID += 1
	}
	return nil
}

func (c *Client) RunClient() {
	client_logger.Println("[run client]-----------------------------")
}
