package shared

// proxy.proto imports orchestrator.proto from the orchestrator package.
//go:generate mise exec -- protoc --proto_path=pkg/grpc/proxy --proto_path=../orchestrator --go_out=pkg/grpc/proxy --go_opt=paths=source_relative --go-grpc_out=pkg/grpc/proxy --go-grpc_opt=paths=source_relative proxy.proto
