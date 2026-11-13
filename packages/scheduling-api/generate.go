package main

//go:generate mise exec -- buf export buf.build/bufbuild/protovalidate:511051f7f4374c3ca873b53ae68a9288 --output .protovalidate
//go:generate mise exec -- protoc --proto_path=. --proto_path=.protovalidate --go_out=../shared --go_opt=module=github.com/e2b-dev/infra/packages/shared --go-grpc_out=../shared --go-grpc_opt=module=github.com/e2b-dev/infra/packages/shared e2b/scheduling/v1/reservation.proto
//go:generate rm -rf .protovalidate
