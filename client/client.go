package client

import (
	"io/ioutil"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
)

const uploadPath = "./tmp"

type Client struct {
	addr       string
	serverAddr string
	client     *rpc.Client
}

func (c Client) RunRpcServer() {

	dialHTTP, err := rpc.DialHTTP("tcp", c.serverAddr)
	if err != nil {
		logger.Println("Connect to server failed")
		return
	}
	c.client = dialHTTP
	return
}

func (c *Client) Download(w http.ResponseWriter, r *http.Request) {

}

func (c *Client) Upload(w http.ResponseWriter, r *http.Request) {

	fileType := r.PostFormValue("type")
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		renderError(w, "INVALID_FILE", http.StatusBadRequest)
		logger.Println("INVALID_FILE", err)
		return
	}
	defer file.Close()
	fileSize := fileHeader.Size
	logger.Printf("File size (bytes): %v\n", fileSize)
	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		renderError(w, "INVALID_FILE", http.StatusBadRequest)
		logger.Println("INVALID_FILE", err)
		return
	}
	fileName := fileHeader.Filename
	newPath := filepath.Join(uploadPath, fileName)
	logger.Printf("FileType: %s, File: %s\n", fileType, newPath)
	newFile, err := os.Create(newPath)
	if err != nil {
		logger.Println("Write file failed", err)
		renderError(w, "CANT_READ_FILE_TYPE", http.StatusInternalServerError)
		return
	}
	defer newFile.Close()
	_, err = newFile.Write(fileBytes)
	if err != nil || newFile.Close() != nil {
		renderError(w, "CANT_READ_FILE_TYPE", http.StatusInternalServerError)
		logger.Println("Write file failed", err)
	}
	w.Write([]byte("SUCCESS"))
}

func (c Client) RunHTTPServer() {
	logger.Println("[run client]-----------------------------")
	http.HandleFunc("/upload", c.Upload)
	http.HandleFunc("/files/", c.Download)
	http.ListenAndServe(c.addr, nil)
}
