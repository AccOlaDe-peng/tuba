FROM public.ecr.aws/docker/library/golang:1.27.1-alpine AS build
ARG TARGET=tuba-api
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tuba ./cmd/${TARGET}

FROM public.ecr.aws/docker/library/alpine:3.22
RUN addgroup -S -g 65532 tuba && adduser -S -D -H -u 65532 -G tuba tuba
COPY --from=build /out/tuba /usr/local/bin/tuba
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/tuba"]
