package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

func fingerprint(request *http.Request, requestContent any) (string, error) {
	encoded, err := json.Marshal(struct {
		Method         string `json:"method"`
		Target         string `json:"target"`
		RequestContent any    `json:"fields"`
	}{request.Method, request.URL.RequestURI(), requestContent})
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(encoded)

	return hex.EncodeToString(hash[:]), nil
}
