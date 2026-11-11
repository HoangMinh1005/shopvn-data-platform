# Community MinIO is source-only. Build the pinned upstream commit, not an unofficial image.
FROM golang:1.24.13-alpine3.22@sha256:3641e0d9b931dc4f2f185dcd669c4679670e9277c8166a838ddb98a2d4389cb5 AS builder
ENV GOMAXPROCS=2 CGO_ENABLED=0
WORKDIR /src
COPY runtime/spike-build/minio-source.tar.gz /tmp/minio.tar.gz
RUN tar -xzf /tmp/minio.tar.gz --strip-components=1 -C /src
RUN go build -p=2 -trimpath -ldflags="-s -w" -o /minio .

FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8
COPY --from=builder /minio /usr/local/bin/minio
USER 1000:1000
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
