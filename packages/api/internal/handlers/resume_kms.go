package handlers

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"hash/crc32"
	"time"

	"github.com/go-jose/go-jose/v4"
	"google.golang.org/api/cloudkms/v1"
)

// Load the pinned key at startup. Resume verification only reads memory.
func loadResumePublicKey(ctx context.Context, version string) (*ecdsa.PublicKey, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := cloudkms.NewService(ctx)
	if err != nil {
		return nil, "", err
	}
	k, err := client.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.GetPublicKey(version).Context(ctx).Do()
	if err != nil {
		return nil, "", err
	}
	if k.Name != version || k.Algorithm != "EC_SIGN_P256_SHA256" || int64(crc32.Checksum([]byte(k.Pem), crc32.MakeTable(crc32.Castagnoli))) != k.PemCrc32c {
		return nil, "", errors.New("unexpected KMS resume public key")
	}
	block, rest := pem.Decode([]byte(k.Pem))
	if block == nil || block.Type != "PUBLIC KEY" || len(rest) != 0 {
		return nil, "", errors.New("invalid KMS public key PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, "", err
	}
	pub, ok := parsed.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, "", errors.New("expected P-256 resume key")
	}
	thumb, err := (&jose.JSONWebKey{Key: pub}).Thumbprint(crypto.SHA256)
	if err != nil {
		return nil, "", err
	}

	return pub, base64.RawURLEncoding.EncodeToString(thumb), nil
}
