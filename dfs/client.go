package dfs

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
)

// Client Client要建立跟namenode和datanode的连接
type Client struct {
	nameNodeAddr   string
	dataNodeAddrs  []string
	dataNodeClient map[string]*rpc.Client // 存储从地址到跟datanode的rpc连接的对象
	nameNodeClient *rpc.Client            // 跟namenode连接
}

func NewClient(nameNodeAddr string) *Client {
	return &Client{
		nameNodeAddr:   nameNodeAddr,
		dataNodeClient: make(map[string]*rpc.Client),
	}
}

func (c *Client) Run() {
	// 先建立连接
	c.Connect()
	fmt.Println("mini-dfs start")

	// 从命令行读取不同指令
	var command string
	for {
		fmt.Printf(">> ")
		fmt.Scan()
		// 从stdin中读取指令直到遇到换行符，停止
		input, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			panic(err)
		}
		command = strings.TrimSpace(input)

		elements := strings.Split(command, " ")
		if len(elements) == 0 {
			continue
		}
		// 有get下载和put上传两种操作
		if elements[0] == "get" {
			// get remote_path local_path 根据远端文件名将文件下载到本地指定地址
			if len(elements) != 3 {
				fmt.Println("miss data dir")
			}
			err := c.Download(elements[1], elements[2])
			if err != nil {
				fmt.Println(err)
			}
		} else if elements[0] == "put" {
			if len(elements) <= 2 {
				fmt.Println("miss data dir")
			}
			target := ""
			if len(elements) > 2 {
				target = elements[2]
			}
			err := c.UploadFile(elements[1], target)
			if err != nil {
				fmt.Println(err)
			}
		} else if elements[0] == "quit" {
			break
		} else {
			fmt.Println("unrecognized command")
		}
	}
	c.Close()
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

func (c *Client) UploadFile(file string, target string) error {

	_, fileName := filepath.Split(file)
	if len(target) != 0 {
		fileName = target
	}
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

func (c *Client) Download(filename string, dst string) error {
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
	f, err := os.Create(dst)
	defer f.Close()
	if err != nil {
		ns_logger.Println("create file failed", err)
		return err
	}
	for i, chunkId := range resp.ChunkId {
		chunkReadRequest := ChunkReadRequest{
			ChunkId: chunkId,
		}
		chunkReadResponse := &ChunkReadResponse{}
		c.dataNodeClient[resp.DataServerAddrs[i]].Call("DataNode.Download", chunkReadRequest, chunkReadResponse)
		decodeString, _ := hex.DecodeString(resp.MD5Code[i])
		if !bytes.Equal(MD5Encode(chunkReadResponse.DATA), decodeString) {
			ns_logger.Println("file checked failed")
			return errors.New("file checked failed")
		}
		f.Write(chunkReadResponse.DATA)
	}
	return nil
}
