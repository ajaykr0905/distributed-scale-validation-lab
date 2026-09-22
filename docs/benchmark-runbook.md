# Reproducible benchmark runbook

Verified results live in `artifacts/` and include every sample, the workload,
adapter selection, environment, and limitations. A checked-in result is a
record of one declared environment, never a universal performance claim.

## Unit-level worker benchmark

```sh
./scripts/run-benchmark.sh | tee benchmark.txt
```

The script records the Go version and operating-system information, then runs
five samples with allocation reporting. `benchmark.txt` is ignored by Git so a
machine-specific result is not accidentally presented as a universal claim.

## End-to-end synthetic run

Build once, then run the same seed and count for each comparison:

```sh
go build -o bin/scale-lab ./cmd/lab
bin/scale-lab -seed public-demo -count 10000 -workers 1
bin/scale-lab -seed public-demo -count 10000 -workers 4
```

For a larger local exercise, change `-count` to the intended synthetic entity
count. Do not describe that count as validated until the command finishes and
the JSON output reports identical `generated` and `stored` values.

## Reporting checklist

- Commit SHA and clean/dirty working-tree state
- CPU model, core count, memory, OS, and Go version
- Exact command, seed, entity count, and worker count
- All samples, not only the fastest result
- Whether the run used memory adapters or separately implemented integrations
- Failures, retries, and resource saturation observed during the run
