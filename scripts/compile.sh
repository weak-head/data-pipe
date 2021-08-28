#!/bin/bash
#
# Protobuf API generation

API_DIR="${API_DIR:-api/v1}"
PROTO_PATH=$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf)

protoc "${API_DIR}"/*.proto \
	--gogo_out=Mgogoproto/gogo.proto=github.com/gogo/protobuf/proto,plugins=grpc:. \
	--proto_path=${PROTO_PATH} \
	--proto_path=.

