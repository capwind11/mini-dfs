package dfs

type FileWriteRequest struct {
	FileName string
	DATA     []byte
}

type FileWriteResponse struct {
	msg    string
	STATUS int
}

type FileReadRequest struct {
	FileName string
}

type FileReadResponse struct {
	FileName string
	DATA     []byte
	msg      string
}

type ChunkWriteRequest struct {
	ChunkId int64
	DATA    []byte
}

type ChunkWriteResponse struct {
	msg    string
	STATUS int
}

type ChunkReadRequest struct {
	ChunkId int64
}

type ChunkReadResponse struct {
	DATA []byte
	msg  string
}

type FileUploadMetaRequest struct {
	FileName string
	ChunkId  int64
}

type FileUploadMetaResponse struct {
	DataServerId int
	ChunkId      int64
	msg          string
}

type FileDownloadMetaRequest struct {
	FileName string
}

type FileDownloadMetaResponse struct {
	DataServerId []int
	ChunkId      []int64
	msg          string
}

type ChunkMetaRequest struct {
	FileName string
	ChunkId  int64
}

type ChunkMetaResponse struct {
	ChunkId int64
	msg     string
}
