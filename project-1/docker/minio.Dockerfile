# MinIO no longer publishes container images, so build the server from
# source at a pinned commit. Only the dev stack uses it; on AWS this is S3.
FROM public.ecr.aws/docker/library/golang:1.26-alpine AS build
ARG MINIO_VERSION=v0.0.0-20260212201848-7aac2a2c5b7c
RUN CGO_ENABLED=0 go install -trimpath -ldflags "-s -w" github.com/minio/minio@${MINIO_VERSION}

FROM public.ecr.aws/docker/library/alpine:3.20
COPY --from=build /go/bin/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
