package client

import (
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

	logger.Printf("download url=%s \n", r.URL.String())
	fileName := r.URL.Query().Get("file")
	if len(fileName) == 0 {
		logger.Println("no file parameter")
		w.Write([]byte("NO FILE PARAMETER"))
		return
	}
	newPath := filepath.Join(uploadPath, fileName)
	f, err := os.Open(newPath)
	if err != nil {
		logger.Println("file open failed")
		w.Write([]byte("FILE OPEN FAILED"))
		return
	}

	io.Copy(w, f)
	return
}

func (c *Client) Upload(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("content-type")
	contentLen := r.ContentLength

	logger.Printf("upload content-type:%s,content-length:%d", contentType, contentLen)
	if !strings.Contains(contentType, "multipart/form-data") {
		logger.Println("content-type must be multipart/form-data")
		w.Write([]byte("content-type must be multipart/form-data"))
		return
	}

	file, header, err := r.FormFile("file")
	defer file.Close()
	if err != nil {
		logger.Println("READ FILE FAILED")
		w.Write([]byte("READ FILE FAILED"))
		return
	}
	newPath := filepath.Join(uploadPath, header.Filename)
	dst, err := os.Create(newPath)
	if err != nil {
		logger.Println("CREATE FILE FAILED")
		w.Write([]byte("CREATE FILE FAILED"))
		return
	}
	defer dst.Close()

	_, err = io.Copy(dst, file)
	if err != nil {
		logger.Println("WRITE FILE FAILED")
		w.Write([]byte("WRITE FILE FAILED"))
		return
	}
	fmt.Printf("successful uploaded,fileName=%s,fileSize=%.2f MB,savePath=%s \n", header.Filename, float64(contentLen)/1024/1024, newPath)

	w.Write([]byte("successful,url=" + url.QueryEscape(header.Filename)))
}

func (c Client) RunHTTPServer() {
	logger.Println("[run client]-----------------------------")
	http.HandleFunc("/upload", c.Upload)
	http.HandleFunc("/files/", c.Download)
	http.ListenAndServe(c.addr, nil)
}
