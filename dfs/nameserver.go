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

type NameServer struct {
	addr           string
	datanodeAddr   []string
	datanodes      []*rpc.Client
	rpcServer      *rpc.Server
	fileMetaData   map[string][]int64 // 格式为filename: {chunk0, chunk1, chunk3}
	chunkMetaData  map[int64][]int    // 格式为chunkId: {ds1, ds2, ds3}
	nextDataServer int
}

func NewNameServer(addr string) *NameServer {
	return &NameServer{
		addr:           addr,
		fileMetaData:   make(map[string][]int64),
		chunkMetaData:  make(map[int64][]int),
		nextDataServer: 0,
	}
}

func (s *NameServer) RunRpcServer() (net.Listener, error) {

	ns_logger.Printf("Run NameServer: %s\n", s.addr)
	server := rpc.NewServer()
	server.Register(s)
	s.rpcServer = server
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		ns_logger.Println("Listen error", err)
		return nil, err
	}
	return listener, http.Serve(listener, server)
}

func (s *NameServer) Upload(req FileUploadMetaRequest, resp *FileUploadMetaResponse) error {
	fileName := req.FileName

	chunk_num := req.FileSize / CHUNK_SIZE
	fileid := db.InsertFile(fileName, chunk_num)
	resp.FileID = fileid
	rest := req.FileSize % CHUNK_SIZE
	if rest != 0 {
		chunk_num += 1
	}
	for chunk_num != 0 {
		datanodeList := []string{s.datanodeAddr[s.nextDataServer], s.datanodeAddr[(s.nextDataServer+1)%4], s.datanodeAddr[(s.nextDataServer+2)%4]}
		chunkID := db.InsertChunk(fileid, fmt.Sprintf("%s;%s;%s", datanodeList[0], datanodeList[1], datanodeList[2]))
		if chunkID == -1 {
			ns_logger.Println("insert chunk fail")
			return errors.New("insert chunk fail")
		}
		resp.ChunkInfo = append(resp.ChunkInfo, ChunkMetaData{
			ChunkId:       chunkID,
			DataNodeAddrs: datanodeList,
		})
		chunk_num -= 1
		s.nextDataServer = (s.nextDataServer + 1) % 4
	}
	return nil
}

func (s *NameServer) Download(req FileDownloadMetaRequest, resp *FileDownloadMetaResponse) error {
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
		dataNodeAddr := strings.Split(chunk.DataNodeAddrs, ";")[rand.Intn(3)]
		resp.DataServerAddrs = append(resp.DataServerAddrs, dataNodeAddr)
	}
	return nil
}

func (n *NameServer) ConnectToNameNode(req DataNodeConnectRequest, resp *DataNodeConnectResponse) error {
	n.datanodeAddr = append(n.datanodeAddr, req.Addr)
	dialHTTP, err := rpc.DialHTTP("tcp", req.Addr)
	if err != nil {
		ns_logger.Println("Connect to DataNode failed")
		return err
	}
	n.datanodes = append(n.datanodes, dialHTTP)
	resp.STATUS = SUCCESS
	return nil
}

func (n *NameServer) CloseConnectionToDataNodes() {
	for _, dn := range n.datanodes {
		dn.Close()
	}
	return
}

func (n *NameServer) RunServer() {
	for {
		select {
		case <-time.After(HeartBeatIntervalTime):
			n.SendHeartBeat()
		}
	}

}

func (n *NameServer) SendHeartBeat() {

	for i, dn := range n.datanodes {
		req := HeartBeatRequest{}
		resp := &HeartBeatResponse{}
		err := dn.Call("DataNode.HeartBeat", req, resp)
		if err != nil {
			// 迁移数据
			n.DataRecovery(n.datanodeAddr[i])
			ns_logger.Printf("datanode:%s failed", n.datanodeAddr[i])
		}
	}
}

func (n *NameServer) DataRecovery(addr string) {
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
		resp := PeerReplicateResponse{}
		n.datanodes[i].Call("DataNode.PeerReplicate", req, resp)
	}
	// for chunkId in 找到唯一不在的datanode 从一个datanode拷贝过去
}
