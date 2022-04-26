package dfs

import (
	"log"
	"net/http"
	"os"
)

var ns_logger = log.New(os.Stdout, "NameServer:", log.Lshortfile)
var ds_logger = log.New(os.Stdout, "DataServer:", log.Lshortfile)
var client_logger = log.New(os.Stdout, "client:", log.Lshortfile)

func renderError(w http.ResponseWriter, message string, statusCode int) {
	w.WriteHeader(statusCode)
	w.Write([]byte(message))
}
