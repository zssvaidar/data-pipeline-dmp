FROM public.ecr.aws/docker/library/golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM public.ecr.aws/docker/library/alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/ /app/
USER app
CMD ["/app/api"]
