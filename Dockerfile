# Build our service
FROM golang:1.17-alpine AS build
WORKDIR /go/src/data-pipe
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/data-pipe ./cmd/data-pipe
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.5 && \
    wget -qO/go/bin/grpc_health_probe \
    https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /go/bin/grpc_health_probe


# Copy the binaries of our service to the new container
FROM scratch
COPY --from=build /go/bin/data-pipe /bin/data-pipe
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
ENTRYPOINT ["/bin/data-pipe"]
