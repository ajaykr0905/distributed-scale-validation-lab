package controlapi

import (
	"mime"
	"net/http"
	"strings"
)

func supportedJSON(request *http.Request) bool {
	contentTypes := request.Header.Values("Content-Type")
	if len(contentTypes) > 1 {
		return false
	}
	if len(contentTypes) == 1 {
		mediaType, params, err := mime.ParseMediaType(contentTypes[0])
		if err != nil || mediaType != "application/json" {
			return false
		}
		if charset, ok := params["charset"]; ok && !strings.EqualFold(charset, "utf-8") {
			return false
		}
	}
	encodings := request.Header.Values("Content-Encoding")
	return len(encodings) == 0 || (len(encodings) == 1 &&
		(strings.TrimSpace(encodings[0]) == "" || strings.EqualFold(strings.TrimSpace(encodings[0]), "identity")))
}
