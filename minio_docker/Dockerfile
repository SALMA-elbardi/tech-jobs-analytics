# Étape 1 : Compilation du binaire MinIO officiel
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /src
RUN git clone --depth 1 --branch RELEASE.2024-05-10T01-41-38Z https://github.com/minio/minio.git . && \
    CGO_ENABLED=0 go build -tags kqueue -trimpath --ldflags '-s -w' -o /go/bin/minio

# Étape 2 : Image d'exécution minimale
FROM alpine:3.19

COPY --from=builder /go/bin/minio /usr/local/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]

ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
