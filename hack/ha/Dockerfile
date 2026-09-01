# Dev-only image for local multi-instance HA testing (hack/ha).
# Not for production. Build context is the repository root.
#
#   docker build -f hack/ha/Dockerfile -t pcsm:dev .

FROM golang:1.27.0@sha256:4013ae0f9e7994f8535c58c811f8f863fbed38b72e0d51e6592156f758d66146 AS build

WORKDIR /src

# Cache module downloads separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary so it runs on the minimal runtime image.
RUN CGO_ENABLED=0 go build -trimpath -o /out/pcsm .

# alpine (not distroless) so `docker exec -it <c> sh` works for poking around.
FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

RUN apk add --no-cache ca-certificates

COPY --from=build /out/pcsm /usr/local/bin/pcsm

ENTRYPOINT ["pcsm"]
