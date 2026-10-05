package handlers

import (
	"strings"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"google.golang.org/grpc/status"
)

// validationMessage reports the response body for a request a contract
// rejected, and whether the status carried such a rejection.
//
// The violations are attached to the status, so they are read from there
// rather than from its message, which renders them as a multi-line dump. A
// response body reads better as one line, and every violation names a field
// the caller sent, so the path is kept as the reader's way back to it.
func validationMessage(grpcStatus *status.Status) (string, bool) {
	for _, detail := range grpcStatus.Details() {
		violations, ok := detail.(*validate.Violations)
		if !ok || len(violations.GetViolations()) == 0 {
			continue
		}

		texts := make([]string, 0, len(violations.GetViolations()))
		for _, violation := range violations.GetViolations() {
			text := violation.GetMessage()
			if path := protovalidate.FieldPathString(violation.GetField()); path != "" {
				text = path + ": " + text
			}
			texts = append(texts, text)
		}

		return strings.Join(texts, "; "), true
	}

	return "", false
}
