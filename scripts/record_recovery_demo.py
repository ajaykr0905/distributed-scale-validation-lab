"""Record an actual public-lab test run as a 90-second captioned terminal replay.

This is an agent-executed terminal-output recording, not a learner presentation
or native desktop capture. Pillow and ffmpeg/ffprobe are optional recording
tools, not backend dependencies. Only fixed disposable lab URLs run.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repository", type=Path, required=True)
parser.add_argument("--project", required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--font", type=Path,
                    default=Path("/System/Library/Fonts/Supplemental/Courier New.ttf"),
                    help="Readable monospace TrueType font; default is macOS Courier New")
args = parser.parse_args()
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError as error:
    raise SystemExit("Missing optional recording dependency: Pillow (see docs/recovery-demo.md)") from error
repository = args.repository.resolve()
output = args.output.resolve()
if not args.font.is_file():
    raise SystemExit("A monospace font is required; supply an existing .ttf file with --font")
if not args.project.startswith("ajay-durable-"):
    raise SystemExit("Only a dedicated ajay-durable-* public Compose project is permitted")
if output.exists() or output.with_suffix(".json").exists():
    raise SystemExit("Recording outputs already exist; choose a new versioned filename")
for command in ("ffmpeg", "ffprobe", "go", "docker", "git"):
    if shutil.which(command) is None:
        raise SystemExit(f"Missing optional recording tool: {command}")
source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=repository, text=True)
if dirty:
    raise SystemExit("Record only a clean committed backend checkout")
services = subprocess.check_output(["docker", "compose", "-p", args.project, "-f", "compose.durable.yaml",
                                    "ps", "--services", "--status", "running"], cwd=repository, text=True).split()
if not {"postgres", "rabbitmq"}.issubset(services):
    raise SystemExit("The explicitly named public lab's postgres/rabbitmq must already be running")
env = os.environ.copy()
env.update({"SCALE_LAB_INTEGRATION": "1", "SCALE_LAB_DOCKER_PROJECT": args.project,
            "SCALE_LAB_POSTGRES_URL": "postgres://scale_lab:local-development-only@127.0.0.1:25432/scale_lab?sslmode=disable",
            "SCALE_LAB_RABBITMQ_URL": "amqp://scale_lab:local-development-only@127.0.0.1:25672/"})
command = ["go", "test", "-race", "-tags=integration", "-count=1", "-timeout=75s", "-v",
           "./internal/durable", "-run", "TestWorkerAndDispatcherProcessDeath|TestIndependentProcessesAndBrokerRestart"]
output.parent.mkdir(parents=True, exist_ok=True)
events = []
started = time.monotonic()
header = ["DURABLE BACKEND | ACTUAL TERMINAL TEST REPLAY", f"Source: {source}",
          "Agent-executed synthetic single-host lab; not learner or production evidence.",
          "API -> PostgreSQL job/outbox -> RabbitMQ -> independent worker -> COMMIT -> ACK"]
visible = []

with tempfile.TemporaryDirectory(prefix="ajay-recovery-recording-") as temporary:
    font = ImageFont.truetype(str(args.font), 17)

    def emit(line):
        elapsed = time.monotonic() - started
        events.append({"elapsed_seconds": elapsed, "text": line})
        visible.extend(textwrap.wrap(line, width=107) or [""])
        frame_path = Path(temporary) / f"frame-{len(events):04d}.png"
        frame = Image.new("RGB", (1280, 720), "#111827")
        draw = ImageDraw.Draw(frame)
        draw.multiline_text((24, 24), "\n".join(header + [""] + visible[-24:]), font=font,
                            fill="white", spacing=4)
        frame.save(frame_path)
        print(line, flush=True)

    emit("$ " + " ".join(command))
    test = subprocess.Popen(command, cwd=repository, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    try:
        for line in test.stdout:
            emit(line.rstrip())
        test.stdout.close()
        code = test.wait(timeout=80)
        emit(f"Exit code: {code}; elapsed wall time: {time.monotonic()-started:.2f}s")
        if code != 0:
            raise RuntimeError("Recovery command failed; recording must not be published")
        emit("PASS: actual subprocess deaths and live broker stop/restart checked.")
        emit("Accounting and recovery timing come from the test output, not generated results.")
        emit("Local fixture only. Reproduce with the pinned source and documented Compose command.")
    finally:
        if test.poll() is None:
            test.terminate()
            test.wait(timeout=10)
    if events[-1]["elapsed_seconds"] >= 90:
        raise RuntimeError("Test output exceeds the requested recording duration")
    timeline = ["ffconcat version 1.0"]
    for index, event in enumerate(events):
        # The first command appears at t=0; subsequent frames preserve the
        # actual output's monotonic event timing, not invented demo timing.
        next_time = events[index + 1]["elapsed_seconds"] if index + 1 < len(events) else 90
        current_time = event["elapsed_seconds"] if index else 0
        timeline.extend([f"file frame-{index + 1:04d}.png", f"duration {next_time-current_time:.9f}"])
    timeline.append(f"file frame-{len(events):04d}.png")
    concat = Path(temporary) / "timeline.ffconcat"
    concat.write_text("\n".join(timeline) + "\n", encoding="utf-8")
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-n", "-safe", "1",
                    "-f", "concat", "-i", str(concat), "-vf", "fps=10,tpad=stop_mode=clone:stop_duration=90", "-t", "90",
                    "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
                    "-movflags", "+faststart", str(output)], check=True, timeout=100)

probe = json.loads(subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration:stream=codec_name,width,height",
                                           "-of", "json", str(output)], text=True))
duration = float(probe["format"]["duration"])
if abs(duration - 90) > 0.1:
    raise RuntimeError(f"Expected 90-second replay, got {duration}")
receipt = {"source_commit": source, "source_dirty": False, "duration_seconds": duration,
           "recording_kind": "Real-time captioned replay of actual agent-executed terminal output; no desktop or learner claim",
           "command": command, "compose_project": args.project, "exit_code": code, "events": events,
           "limitations": ["Single host; Docker broker/database; synthetic fixtures.", "No voice/narration; final successful output held until 90 seconds."]}
output.with_suffix(".json").write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
print(f"Verified {duration:.1f}s MP4 plus complete timestamped output receipt: {output}")
