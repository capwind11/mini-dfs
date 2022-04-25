package client

import "testing"

func TestRunClient(t *testing.T) {
	addr := "127.0.0.1:8000"
	serverAddr := "127.0.0.1:8080"
	client := Client{
		addr:       addr,
		serverAddr: serverAddr,
	}

	client.RunHTTPServer()
}
