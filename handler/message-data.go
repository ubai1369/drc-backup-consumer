package handler

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"strings"
)

type NsqBackupDataReq struct {
	Path   string              `json:"path"`
	Method string              `json:"method"`
	Header map[string][]string `json:"header"`
	Body   []byte              `json:"body"`
}

func (n NsqBackupDataReq) SendReq() error {
	switch strings.ToUpper(n.Method) {
	case "POST", "PUT", "DELETE":
		baseUrl := os.Getenv("DRC_BASE_ENDPOINT")
		url := baseUrl + n.Path
		req, err := http.NewRequest(n.Method, url, bytes.NewBuffer(n.Body))
		if err != nil {
			return err
		}

		// Set headers
		req.Header = n.Header

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
	default:
		return errors.New("invalid request method:" + n.Method)
	}

	return nil
}
