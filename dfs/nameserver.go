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
)

type NameServer struct {
	addr           string
	datanodeAddr   []string
	rpcServer      *rpc.Server
	fileMetaData   map[string][]int64 // 格式为filename: {chunk0, chunk1, chunk3}
	chunkMetaData  map[int64][]int    // 格式为chunkId: {ds1, ds2, ds3}
	nextDataServer int
}

func NewNameServer(addr string, datanodeAddr []string) *NameServer {
	return &NameServer{
		addr:           addr,
		datanodeAddr:   datanodeAddr,
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
		dataNodeAddr := strings.Split(chunk.DATANODE_ADRRS, ";")[rand.Intn(3)]
		resp.DataServerAddrs = append(resp.DataServerAddrs, dataNodeAddr)
	}
	return nil
}
