FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/scale-lab ./cmd/lab

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/scale-lab /scale-lab
ENTRYPOINT ["/scale-lab"]
