package consts

import (
	"encoding/base64"
	"fmt"
	"os"
)

var (
	GCPProject                  = os.Getenv("GCP_PROJECT_ID")
	Domain                      = os.Getenv("DOMAIN_NAME")
	GoogleServiceAccountSecret  = os.Getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
	GoogleSigningServiceAccount = os.Getenv("GCP_SIGNING_SERVICE_ACCOUNT")
	DockerAuthConfig            = os.Getenv("DOCKER_AUTH_BASE64")
	GCPRegion                   = os.Getenv("GCP_REGION")
)

var EncodedDockerCredentials = base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "_json_key_base64:%s", GoogleServiceAccountSecret))
