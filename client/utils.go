package client

import (
	"log"
	"net/http"
	"os"
)

var logger = log.New(os.Stdout, "client:", log.Lshortfile)

func renderError(w http.ResponseWriter, message string, statusCode int) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(message))
}
