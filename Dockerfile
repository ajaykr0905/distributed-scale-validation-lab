FROM golang:1.26.4-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/scale-lab ./cmd/lab \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/durable ./cmd/durable

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/scale-lab /scale-lab
COPY --from=build /out/durable /durable
ENTRYPOINT ["/scale-lab"]
