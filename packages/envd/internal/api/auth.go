package api

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/awnumar/memguard"

	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

const (
	SigningReadOperation  = "read"
	SigningWriteOperation = "write"

	accessTokenHeader = "X-Access-Token"
)

// paths that are always allowed without general authentication
// POST/init is secured via MMDS hash validation instead
var authExcludedPaths = []string{
	"GET/health",
	"GET/files",
	"POST/files",
	"POST/init",
}

// handoverPreInitAllowedPaths is the MINIMAL set reachable on a live-upgraded
// envd before its post-upgrade /init has restored the access token: only /init
// (which restores auth and lifts this gate, self-authenticated via MMDS) and
// the health check. It deliberately omits /files — unlike authExcludedPaths —
// so a re-adopted (possibly hostile) guest process can't reach the
// root-privileged file API unauthenticated in that window. The orchestrator
// delivers the upgrade over /upgrade's body, not /files, so nothing legitimate
// needs /files before /init.
var handoverPreInitAllowedPaths = []string{
	"GET/health",
	"POST/init",
}

func (a *API) WithAuthorization(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// check if this path is allowed without authentication (e.g., health check, endpoints supporting signing)
		allowedPath := slices.Contains(authExcludedPaths, req.Method+req.URL.Path)

		switch {
		case a.accessToken.IsSet():
			authHeader := req.Header.Get(accessTokenHeader)

			if !a.accessToken.Equals(authHeader) && !allowedPath {
				a.logger.Error().Msg("Trying to access secured envd without correct access token")

				err := errors.New("unauthorized access, please provide a valid access token or method signing if supported")
				jsonError(w, http.StatusUnauthorized, err)

				return
			}

		case a.handover != nil && !a.initialized.Load() && !slices.Contains(handoverPreInitAllowedPaths, req.Method+req.URL.Path):
			// A live-upgraded envd serves before its post-upgrade /init has
			// restored the access token — and the fallback thaw may already be
			// running the re-adopted workload. The sandbox HAD a token (it is a
			// resume), so treat the unset token as "not yet restored" and fail
			// CLOSED here rather than falling through to the open path below,
			// which would let a re-adopted (and possibly hostile) guest process
			// reach control endpoints unauthenticated in this window. Only the
			// minimal handoverPreInitAllowedPaths (/init, /health) get through —
			// notably NOT /files, whose root file API is otherwise unauthenticated
			// (it is in authExcludedPaths). /init self-authenticates via MMDS and
			// lifts this gate.
			a.logger.Warn().Msg("blocking pre-init request on live-upgraded envd (auth not yet restored)")

			jsonError(w, http.StatusUnauthorized, errors.New("envd not initialized"))

			return
		}

		handler.ServeHTTP(w, req)
	})
}

func (a *API) generateSignature(path string, username string, operation string, signatureExpiration *int64) (string, error) {
	tokenBytes, err := a.accessToken.Bytes()
	if err != nil {
		return "", fmt.Errorf("access token is not set: %w", err)
	}
	defer memguard.WipeBytes(tokenBytes)

	var signature string
	hasher := keys.NewSHA256Hashing()

	if signatureExpiration == nil {
		signature = strings.Join([]string{path, operation, username, string(tokenBytes)}, ":")
	} else {
		signature = strings.Join([]string{path, operation, username, string(tokenBytes), strconv.FormatInt(*signatureExpiration, 10)}, ":")
	}

	return fmt.Sprintf("v1_%s", hasher.HashWithoutPrefix([]byte(signature))), nil
}

func (a *API) validateSigning(r *http.Request, signature *string, signatureExpiration *int, username *string, path string, operation string) (err error) {
	var expectedSignature string

	// no need to validate signing key if access token is not set
	if !a.accessToken.IsSet() {
		return nil
	}

	// check if access token is sent in the header
	tokenFromHeader := r.Header.Get(accessTokenHeader)
	if tokenFromHeader != "" {
		if !a.accessToken.Equals(tokenFromHeader) {
			return errors.New("access token present in header but does not match")
		}

		return nil
	}

	if signature == nil {
		return errors.New("missing signature query parameter")
	}

	// Empty string is used when no username is provided and the default user should be used
	signatureUsername := ""
	if username != nil {
		signatureUsername = *username
	}

	if signatureExpiration == nil {
		expectedSignature, err = a.generateSignature(path, signatureUsername, operation, nil)
	} else {
		exp := int64(*signatureExpiration)
		expectedSignature, err = a.generateSignature(path, signatureUsername, operation, &exp)
	}

	if err != nil {
		a.logger.Error().Err(err).Msg("error generating signing key")

		return errors.New("invalid signature")
	}

	// Check expiration before HMAC comparison. Doing the expiration check
	// after ConstantTimeCompare leaks a timing oracle: a correct-but-expired
	// signature takes measurably longer than an incorrect one because only the
	// former reaches this block. Moving expiration first ensures the HMAC is
	// only evaluated for non-expired requests, eliminating the side-channel.
	if signatureExpiration != nil {
		exp := int64(*signatureExpiration)
		if exp < time.Now().Unix() {
			return errors.New("signature is already expired")
		}
	}

	// HMAC validation -- constant-time comparison to prevent timing attacks.
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(*signature)) != 1 {
		return errors.New("invalid signature")
	}

	return nil
}
