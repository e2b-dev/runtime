package main

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTemporaryIngressAllowlist(t *testing.T) {
	forwarded := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded++
		w.WriteHeader(http.StatusNoContent)
	})
	tests := []struct {
		name, mode, method, path string
		headers                  map[string]string
		forward                  bool
	}{
		{"control create", "control", "POST", "/sandboxes", map[string]string{"X-API-Key": strings.Repeat("a", 32)}, true},
		{"control admin denied", "control", "GET", "/admin", map[string]string{"X-API-Key": strings.Repeat("a", 32)}, false},
		{"control wrong key", "control", "POST", "/sandboxes", map[string]string{"X-API-Key": "wrong"}, false},
		{"control encoded colon key", "control", "GET", "/v1/cathedral/operations/cathedral%3Aabc123", map[string]string{"X-API-Key": strings.Repeat("a", 32)}, true},
		{"control encoded slash key", "control", "GET", "/v1/cathedral/operations/cathedral%2Fabc123", map[string]string{"X-API-Key": strings.Repeat("a", 32)}, false},
		{"control double encoded slash", "control", "GET", "/v1/cathedral/operations/cathedral%252Fabc123", map[string]string{"X-API-Key": strings.Repeat("a", 32)}, false},
		{"control encoded bypass", "control", "GET", "/sandboxes/%2e%2e/admin", map[string]string{"X-API-Key": strings.Repeat("a", 32)}, false},
		{"guest process", "guest", "POST", "/process.Process/Start", map[string]string{"E2b-Sandbox-Id": "i123", "E2b-Sandbox-Port": "49983", "X-Access-Token": "token"}, true},
		{"guest init denied", "guest", "POST", "/init", map[string]string{"E2b-Sandbox-Id": "i123", "E2b-Sandbox-Port": "49983", "X-Access-Token": "token"}, false},
		{"guest other port denied", "guest", "POST", "/process.Process/Start", map[string]string{"E2b-Sandbox-Id": "i123", "E2b-Sandbox-Port": "8080", "X-Access-Token": "token"}, false},
		{"guest missing token", "guest", "GET", "/files?path=x", map[string]string{"E2b-Sandbox-Id": "i123", "E2b-Sandbox-Port": "49983"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := forwarded
			r := httptest.NewRequest(tt.method, tt.path, nil)
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			handler(tt.mode, []byte(strings.Repeat("a", 32)), upstream).ServeHTTP(w, r)
			if (forwarded > before) != tt.forward {
				t.Fatalf("forwarded=%t, expected %t", forwarded > before, tt.forward)
			}
		})
	}
}

func TestTemporaryIngressGuestBasicRoot(t *testing.T) {
	guestHeaders := map[string]string{
		"E2b-Sandbox-Id": "i123", "E2b-Sandbox-Port": "49983", "X-Access-Token": "token",
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("root:")) {
			t.Errorf("root Basic auth was not forwarded")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	for _, tt := range []struct {
		name, authorization string
		allowed             bool
	}{
		{"root", "Basic " + base64.StdEncoding.EncodeToString([]byte("root:")), true},
		{"other user", "Basic " + base64.StdEncoding.EncodeToString([]byte("user:")), false},
		{"root password", "Basic " + base64.StdEncoding.EncodeToString([]byte("root:password")), false},
		{"bearer", "Bearer token", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/process.Process/Start", nil)
			for k, v := range guestHeaders {
				r.Header.Set(k, v)
			}
			r.Header.Set("Authorization", tt.authorization)
			w := httptest.NewRecorder()
			handler("guest", nil, upstream).ServeHTTP(w, r)
			if (w.Code == http.StatusNoContent) != tt.allowed {
				t.Fatalf("status=%d, allowed=%t", w.Code, tt.allowed)
			}
		})
	}
}
