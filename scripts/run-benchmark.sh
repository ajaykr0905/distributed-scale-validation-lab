#!/bin/sh
set -eu

go version
uname -a
go test -run '^$' -bench '^BenchmarkProcessOne$' -benchmem -count 5 ./internal/worker
