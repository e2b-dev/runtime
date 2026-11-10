package main

//go:generate mise exec -- protoc --go_out=../shared/pkg/grpc/orchestrator/ --go_opt=paths=source_relative --go-grpc_out=../shared/pkg/grpc/orchestrator/ --go-grpc_opt=paths=source_relative orchestrator.proto
//go:generate mise exec -- protoc --go_out=../shared/pkg/grpc/orchestrator/ --go_opt=paths=source_relative --go-grpc_out=../shared/pkg/grpc/orchestrator/ --go-grpc_opt=paths=source_relative volume.proto
//go:generate mise exec -- protoc --go_out=../shared/pkg/grpc/orchestrator/ --go_opt=paths=source_relative --go-grpc_out=../shared/pkg/grpc/orchestrator/ --go-grpc_opt=paths=source_relative chunks.proto
//go:generate mise exec -- protoc --go_out=../shared/pkg/grpc/orchestrator-info/ --go_opt=paths=source_relative --go-grpc_out=../shared/pkg/grpc/orchestrator-info/ --go-grpc_opt=paths=source_relative info.proto
//go:generate mise exec -- protoc --go_out=../shared/pkg/grpc/template-manager/ --go_opt=paths=source_relative --go-grpc_out=../shared/pkg/grpc/template-manager/ --go-grpc_opt=paths=source_relative template-manager.proto
//go:generate mise exec -- env GOOS=linux mockery
