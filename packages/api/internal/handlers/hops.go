package handlers

import "google.golang.org/grpc/credentials"

// Hops are the transport credentials of the api's gRPC client hops, one
// per hop, so each reports under its own name.
type Hops struct {
	SecretsStore credentials.TransportCredentials
	Webhooks     credentials.TransportCredentials
	Analytics    credentials.TransportCredentials
	Orchestrator credentials.TransportCredentials
}
