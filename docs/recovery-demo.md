# Recorded recovery checks

[Watch the 90-second recovery replay](../artifacts/2026-10-08-recovery-demo.mp4)
and inspect its [complete timestamped output receipt](../artifacts/2026-10-08-recovery-demo.json).
This is a captioned replay of actual **agent-executed terminal test output**,
not a desktop capture, learner presentation, or production incident. The tests
ran against clean source `6cc588b72cb44258d65ea12e6f18ec281e9ea033`; their output
finishes after about 13 seconds, then the final successful frame is held until
90 seconds. There is no narration.

The recording exercises actual helper subprocesses exiting before result commit,
after commit but before acknowledgement, and after outbox publication but before
its database commit. It also launches independent worker/dispatcher processes,
stops and restarts the dedicated Compose broker, accepts a job during the outage,
checks durable accounting and duplicate handling, and requires graceful exit.

The recorded broker recovery is **5.119233 seconds**, including Compose restart
and health wait. The [separate benchmark recovery receipt](../artifacts/2026-10-08-recovery-v2.json)
records **3.401676166 seconds** from a different trial. Neither overwrites the
other; these are single-host observations, not a recovery SLA. The fixed URLs,
credentials, schemas, and jobs are disposable public fixtures.

## Reproduce the checks without recording tools

Go and Docker Compose suffice for the backend. Run only the explicitly named
public lab project; the test deliberately stops its RabbitMQ service. The pinned
images and fixed localhost ports are in `compose.durable.yaml`. Do not start a
second project on those ports while another lab instance is running.

```sh
docker compose -p ajay-durable-demo -f compose.durable.yaml up -d --wait
export SCALE_LAB_INTEGRATION=1
export SCALE_LAB_DOCKER_PROJECT=ajay-durable-demo
export SCALE_LAB_POSTGRES_URL='postgres://scale_lab:local-development-only@127.0.0.1:25432/scale_lab?sslmode=disable'
export SCALE_LAB_RABBITMQ_URL='amqp://scale_lab:local-development-only@127.0.0.1:25672/'
go test -race -tags=integration -count=1 -timeout=75s -v ./internal/durable \
  -run 'TestWorkerAndDispatcherProcessDeath|TestIndependentProcessesAndBrokerRestart'
docker compose -p ajay-durable-demo -f compose.durable.yaml down
```

## Make a new timestamped replay

The optional [recorder](../scripts/record_recovery_demo.py) runs those exact tests,
retains their monotonically timed output, renders each output frame, and verifies
the 90-second H.264 MP4 with `ffprobe`. A failed test, dirty checkout, missing
services, or existing output filename stops recording. Use a new output filename
outside the checkout; recording files inside it would make a later run dirty.

Optional dependencies are Python 3, Pillow, and an `ffmpeg` installation providing
`ffprobe` and the `libx264` encoder. The helper was checked locally with Pillow
12.3.0 and FFmpeg 9.0.2 on macOS; capture-time tool versions are not in the original
receipt. These are not Go/backend runtime dependencies. An isolated optional
Python environment can be prepared with:

```sh
python3 -m venv /tmp/ajay-recovery-recording-tools
/tmp/ajay-recovery-recording-tools/bin/python -m pip install 'Pillow==12.3.0'
/tmp/ajay-recovery-recording-tools/bin/python scripts/record_recovery_demo.py \
  --repository "$PWD" --project ajay-durable-demo \
  --output /tmp/ajay-recovery-new.mp4
```

The default font is macOS Courier New at
`/System/Library/Fonts/Supplemental/Courier New.ttf`. On Linux or another platform,
pass an installed readable monospace TrueType font, for example
`--font /usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf`. Confirm the chosen
font fits the 1280x720 output before sharing. The recorder does not install fonts
or FFmpeg, capture browser/desktop content, or read employer systems. Stop only
the dedicated lab project after recording; named volumes retain synthetic data.
