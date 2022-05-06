package dfs

import (
	"fmt"
	"math/rand"
	"mini-dfs/db"
	"net"
	"net/http"
	"net/rpc"
)

type NameServer struct {
	addr           string
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
	fileid := db.InsertFile(fileName)

	datanodeList := fmt.Sprintf("%d;%d;%d", s.nextDataServer, (s.nextDataServer+1)%4, (s.nextDataServer+2)%4)
	chunkID := db.InsertChunk(fileid, datanodeList)
	if _, ok := s.fileMetaData[fileName]; ok {
		s.fileMetaData[fileName] = append(s.fileMetaData[fileName], chunkID)
	} else {
		s.fileMetaData[fileName] = []int64{chunkID}
	}
	s.chunkMetaData[chunkID] = []int{s.nextDataServer, (s.nextDataServer + 1) % 4, (s.nextDataServer + 2) % 4}
	resp.ChunkId = chunkID
	resp.DataServerId = s.nextDataServer
	resp.msg = fmt.Sprintf("file: %s, chunK: %d is allocated as chunkId: %d dataserver: %v", fileName, req.ChunkId, chunkID, datanodeList)
	s.nextDataServer = (s.nextDataServer + 1) % 4
	return nil
}

func (s *NameServer) Download(req FileDownloadMetaRequest, resp *FileDownloadMetaResponse) error {
	fileName := req.FileName

	if v, ok := s.fileMetaData[fileName]; ok {
		for _, chunkID := range v {
			resp.ChunkId = append(resp.ChunkId, chunkID)
			dataServerId := s.chunkMetaData[chunkID][rand.Intn(3)]
			resp.DataServerId = append(resp.DataServerId, dataServerId)
		}
	} else {
		msg := fmt.Sprintf("file %s not exist\n", req.FileName)
		ns_logger.Printf(msg)
		resp.msg = msg
	}
	return nil
}
