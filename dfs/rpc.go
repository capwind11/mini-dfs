package dfs

type ChunkMetaData struct {
	ChunkId       int64
	DataNodeAddrs []string
}

type DataNodeConnectRequest struct {
	Addr string
}

type DataNodeConnectResponse struct {
	STATUS int
	Id     int
}

type HeartBeatRequest struct {
}

type HeartBeatResponse struct {
	STATUS int
}

type ChunkWriteRequest struct {
	ChunkId   int64
	DATA      []byte
	DataNodes []string
	MD5Code   []byte
}

type ChunkWriteResponse struct {
	msg    string
	STATUS int
}

type ChunkReadRequest struct {
	ChunkId int64
}

type ChunkReadResponse struct {
	DATA    []byte
	MD5Code []byte
	msg     string
}

type FileUploadMetaRequest struct {
	FileName string
	FileSize int64
}

type FileUploadMetaResponse struct {
	FileID    int64
	ChunkInfo []ChunkMetaData
}

type FileDownloadMetaRequest struct {
	FileName string
}

type FileDownloadMetaResponse struct {
	DataServerAddrs []string
	ChunkId         []int64
	MD5Code         []string
}
type PeerReplicateRequest struct {
	ChunkId         []int64
	DataServerAddrs []string
	MD5Code         []string
}

type PeerReplicateResponse struct {
	Status int
}

type DataNodeInfoReq struct {
}

type DataNodeInfoResp struct {
	Addrs []string
}
