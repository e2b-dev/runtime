// Package cabundle parses delivered certificates and reconciles them with shared trust.
package cabundle

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// Certificate holds a DER identity and normalized PEM without block headers.
type Certificate struct {
	Fingerprint string
	PEM         []byte
}

// Bundle contains distinct certificates. Treat parsed bundles as immutable.
type Bundle []Certificate

// Parse rejects partial or malformed delivered bundles and removes duplicates.
func Parse(bundlePEM string) (Bundle, error) {
	var certs Bundle
	seen := make(map[string]struct{})
	for rest := bytes.TrimSpace([]byte(bundlePEM)); len(rest) > 0; {
		block, tail := pem.Decode(rest)
		// Decode skips malformed prefixes. Reject them rather than silently
		// installing only part of the delivered set.
		if block == nil || !bytes.HasPrefix(rest, []byte("-----BEGIN ")) || bytes.Count(rest[:len(rest)-len(tail)], []byte("-----BEGIN ")) != 1 {
			return nil, errors.New("malformed certificate PEM")
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("PEM block is %q, not a certificate", block.Type)
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return nil, fmt.Errorf("parse certificate: %w", err)
		}
		id := fmt.Sprintf("%x", sha256.Sum256(block.Bytes))
		if _, duplicate := seen[id]; !duplicate {
			certs = append(certs, Certificate{Fingerprint: id, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes})})
			seen[id] = struct{}{}
		}
		rest = bytes.TrimSpace(tail)
	}
	if len(certs) == 0 {
		return nil, errors.New("no certificates")
	}

	return certs, nil
}

// Equal compares certificate identities regardless of order or PEM formatting.
func (a Bundle) Equal(b Bundle) bool {
	if len(a) != len(b) {
		return false
	}
	set := map[string]struct{}{}
	for _, cert := range a {
		set[cert.Fingerprint] = struct{}{}
	}
	for _, cert := range b {
		if _, ok := set[cert.Fingerprint]; !ok {
			return false
		}
	}

	return true
}

// Reconcile removes owned identities from the aggregate, then appends the desired set.
// Unrelated certificates retain their trust regardless of their source.
func Reconcile(content []byte, owned []string, desired Bundle) []byte {
	managed := make(map[string]struct{}, len(owned)+len(desired))
	for _, id := range owned {
		managed[id] = struct{}{}
	}
	for _, cert := range desired {
		managed[cert.Fingerprint] = struct{}{}
	}
	var out bytes.Buffer
	for rest := content; len(rest) > 0; {
		block, tail := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = tail
		if block.Type == "CERTIFICATE" {
			id := fmt.Sprintf("%x", sha256.Sum256(block.Bytes))
			if _, remove := managed[id]; remove {
				continue
			}
		}
		_ = pem.Encode(&out, block)
	}
	for _, cert := range desired {
		out.Write(cert.PEM)
	}

	return out.Bytes()
}
