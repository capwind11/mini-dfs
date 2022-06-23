package dfs

import (
	"errors"
	"fmt"
	"math/rand"
	"mini-dfs/db"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

type NameNode struct {
	addr           string
	datanodeAddr   []string
	datanodes      []*rpc.Client
	rpcServer      *rpc.Server
	fileMetaData   map[string][]int64 // 格式为filename: {chunk0, chunk1, chunk3}
	chunkMetaData  map[int64][]int    // 格式为chunkId: {ds1, ds2, ds3}
	nextDataServer int
}

func NewNameServer(addr string) *NameNode {
	return &NameNode{
		addr:           addr,
		fileMetaData:   make(map[string][]int64),
		chunkMetaData:  make(map[int64][]int),
		nextDataServer: 0,
	}
}

func (n *NameNode) Run() {
	ns_logger.Printf("Run NameNode: %s\n", n.addr)
	server := rpc.NewServer()
	server.Register(n)
	n.rpcServer = server
	listener, err := net.Listen("tcp", n.addr)
	if err != nil {
		ns_logger.Println("Listen error", err)
	}
	go http.Serve(listener, server)
	for {
		select {
		case <-time.After(HeartBeatIntervalTime):
			n.SendHeartBeat()
		}
	}
}

func (n *NameNode) RunRpcServer() (net.Listener, error) {

	ns_logger.Printf("Run NameNode: %s\n", n.addr)
	server := rpc.NewServer()
	server.Register(n)
	n.rpcServer = server
	listener, err := net.Listen("tcp", n.addr)
	if err != nil {
		ns_logger.Println("Listen error", err)
		return nil, err
	}
	return listener, http.Serve(listener, server)
}

func (n *NameNode) Upload(req FileUploadMetaRequest, resp *FileUploadMetaResponse) error {
	fileName := req.FileName
	REPLICATE_NUM = len(n.datanodes) - 1
	chunk_num := req.FileSize / CHUNK_SIZE
	fileid := db.InsertFile(fileName, chunk_num)
	resp.FileID = fileid
	rest := req.FileSize % CHUNK_SIZE
	if rest != 0 {
		chunk_num += 1
	}
	for chunk_num != 0 {
		datanodeList := make([]string, 0) //, s.datanodeAddr[(s.nextDataServer+1)%4], s.datanodeAddr[(s.nextDataServer+2)%4]}
		for i := 0; i < REPLICATE_NUM; i += 1 {
			datanodeList = append(datanodeList, n.datanodeAddr[(n.nextDataServer+i)%len(n.datanodeAddr)])
		}

		chunkID := db.InsertChunk(fileid, strings.Join(datanodeList, ";"))
		if chunkID == -1 {
			ns_logger.Println("insert chunk fail")
			return errors.New("insert chunk fail")
		}
		resp.ChunkInfo = append(resp.ChunkInfo, ChunkMetaData{
			ChunkId:       chunkID,
			DataNodeAddrs: datanodeList,
		})
		chunk_num -= 1
		n.nextDataServer = (n.nextDataServer + 1) % len(n.datanodeAddr)
	}
	return nil
}

func (n *NameNode) Download(req FileDownloadMetaRequest, resp *FileDownloadMetaResponse) error {
	fileName := req.FileName
	chunkInfo := db.QueryFile(fileName)
	if chunkInfo == nil {
		msg := fmt.Sprintf("file %s metadata query failed\n", req.FileName)
		ns_logger.Printf(msg)
		return errors.New(msg)
	}
	for _, chunk := range chunkInfo {
		resp.ChunkId = append(resp.ChunkId, chunk.Id)
		resp.MD5Code = append(resp.MD5Code, chunk.MD5CODE)
		splitAddrs := strings.Split(chunk.DataNodeAddrs, ";")
		dataNodeAddr := splitAddrs[rand.Intn(len(splitAddrs))]
		resp.DataServerAddrs = append(resp.DataServerAddrs, dataNodeAddr)
	}
	return nil
}

func (n *NameNode) ConnectToNameNode(req DataNodeConnectRequest, resp *DataNodeConnectResponse) error {
	n.datanodeAddr = append(n.datanodeAddr, req.Addr)
	dialHTTP, err := rpc.DialHTTP("tcp", req.Addr)
	if err != nil {
		ns_logger.Println("Connect to DataNode failed")
		return err
	}
	n.datanodes = append(n.datanodes, dialHTTP)
	resp.STATUS = SUCCESS
	resp.Id = len(n.datanodes) - 1
	return nil
}

func (n *NameNode) CloseConnectionToDataNodes() {
	for _, dn := range n.datanodes {
		dn.Close()
	}
	return
}

func (n *NameNode) RunServer() {
	for {
		select {
		case <-time.After(HeartBeatIntervalTime):
			n.SendHeartBeat()
		}
	}
}

func (n *NameNode) GetDataNodeAddrs(req DataNodeInfoReq, resp *DataNodeInfoResp) error {
	for _, addr := range n.datanodeAddr {
		resp.Addrs = append(resp.Addrs, addr)
	}
	return nil
}

// SendHeartBeat 发送心跳包
func (n *NameNode) SendHeartBeat() {

	for i, dn := range n.datanodes {
		req := HeartBeatRequest{}
		resp := &HeartBeatResponse{}
		err := dn.Call("DataNode.HeartBeat", req, resp)
		if err != nil {

			ns_logger.Printf("datanode:%s failed", n.datanodeAddr[i])

			// 如果未收到来自i号DataNode的心跳回复，则启动恢复任务
			n.DataRecovery(n.datanodeAddr[i])

			// 将已失效的i号DataNode从健康连接的列表中删除
			n.datanodeAddr = append(n.datanodeAddr[:i], n.datanodeAddr[i+1:]...)
			n.datanodes = append(n.datanodes[:i], n.datanodes[i+1:]...)
		}
	}
}

func (n *NameNode) DataRecovery(addr string) {
	chunkIds := db.QueryChunkOnDataNode(addr)
	db.DeleteChunkOnDataNode(addr)
	chunks := db.QueryChunks(chunkIds)
	var source int
	var target int
	datanode2chunk := map[int]*PeerReplicateRequest{}
	for _, chunk := range chunks {
		addrs := strings.Split(chunk.DataNodeAddrs, ";")
		for i, ad := range n.datanodeAddr {
			_, ok := Find(addrs, ad)
			if !ok {
				target = i
				break
			}
		}
		for _, ad := range addrs {
			if ad != addr {
				source, _ = Find(n.datanodeAddr, ad)
				break
			}
		}
		for i, ad := range addrs {
			if ad == addr {
				addrs[i] = n.datanodeAddr[target]
				break
			}
		}

		_, ok := datanode2chunk[source]
		if !ok {
			datanode2chunk[source] = &PeerReplicateRequest{
				ChunkId:         make([]int64, 0),
				MD5Code:         make([]string, 0),
				DataServerAddrs: make([]string, 0),
			}
		}
		db.UpdateChunkDataNode(chunk.Id, strings.Join(addrs, ";"))
		datanode2chunk[source].ChunkId = append(datanode2chunk[source].ChunkId, chunk.Id)
		datanode2chunk[source].MD5Code = append(datanode2chunk[source].MD5Code, chunk.MD5CODE)
		datanode2chunk[source].DataServerAddrs = append(datanode2chunk[source].DataServerAddrs, n.datanodeAddr[target])
	}
	for i, req := range datanode2chunk {
		peerReplicateRequest := PeerReplicateRequest{
			ChunkId:         req.ChunkId,
			MD5Code:         req.MD5Code,
			DataServerAddrs: req.DataServerAddrs,
		}
		resp := &PeerReplicateResponse{}
		err := n.datanodes[i].Call("DataNode.PeerReplicate", peerReplicateRequest, resp)
		if err != nil {
			ns_logger.Println(err)
			return
		}
	}
}
