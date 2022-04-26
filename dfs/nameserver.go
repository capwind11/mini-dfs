package dfs

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type NameServer struct {
	addr           string
	rpcServer      *rpc.Server
	fileMetaData   map[string][]int64 // 格式为filename: {chunk0, chunk1, chunk3}
	chunkMetaData  map[int64][]int    // 格式为chunkId: {ds1, ds2, ds3}
	nextChunkId    int64
	nextDataServer int
}

func NewNameServer(addr string) *NameServer {
	return &NameServer{
		addr:           addr,
		fileMetaData:   make(map[string][]int64),
		chunkMetaData:  make(map[int64][]int),
		nextChunkId:    0,
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

func (s *NameServer) Upload(req FileMetaRequest, resp *FileMetaResponse) error {
	fileName := req.FileName

	if v, ok := s.fileMetaData[fileName]; ok {
		v = append(v, s.nextChunkId)
	} else {
		s.fileMetaData[fileName] = []int64{s.nextChunkId}
	}

	s.chunkMetaData[s.nextChunkId] = []int{s.nextDataServer, s.nextDataServer + 1, s.nextDataServer + 2}

	resp.ChunkId = s.nextChunkId
	resp.DataServerId = s.nextDataServer
	resp.msg = fmt.Sprintf("file: %s, chunK: %d is allocated as chunkId: %d dataserver: %d", fileName, req.ChunkId, s.nextChunkId, s.nextDataServer)
	s.nextDataServer = (s.nextDataServer + 1) % 4
	s.nextChunkId += 1
	return nil
}
