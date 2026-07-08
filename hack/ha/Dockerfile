# Dev-only image for local multi-instance HA testing (hack/ha).
# Not for production. Build context is the repository root.
#
#   docker build -f hack/ha/Dockerfile -t pcsm:dev .

FROM golang:1.26 AS build

WORKDIR /src

# Cache module downloads separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary so it runs on the minimal runtime image.
RUN CGO_ENABLED=0 go build -trimpath -o /out/pcsm .

# alpine (not distroless) so `docker exec -it <c> sh` works for poking around.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=build /out/pcsm /usr/local/bin/pcsm

ENTRYPOINT ["pcsm"]
