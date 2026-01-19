#!/usr/bin/env bash
set -euo pipefail

: "${RTMP_URL:?RTMP_URL env var is required}"
HLS_DIR="${HLS_DIR:-/hls}"
mkdir -p "${HLS_DIR}"

PLAYLIST="${HLS_DIR}/index.m3u8"
SEGMENT_PATTERN="${HLS_DIR}/segment_%05d.ts"

ffmpeg_pid=""

cleanup() {
  # Forward TERM to ffmpeg if we're not using exec / if it hasn't replaced us
  if [[ -n "${ffmpeg_pid}" ]] && kill -0 "${ffmpeg_pid}" 2>/dev/null; then
    kill -TERM "${ffmpeg_pid}" 2>/dev/null || true
    # give it a moment, then force
    for _ in {1..10}; do
      kill -0 "${ffmpeg_pid}" 2>/dev/null || exit 0
      sleep 0.2
    done
    kill -KILL "${ffmpeg_pid}" 2>/dev/null || true
  fi
}
trap cleanup TERM INT

ffmpeg \
  -hide_banner \
  -loglevel info \
  -thread_queue_size 1024 \
  -nostdin \
  -i "${RTMP_URL}" \
  -c:v copy \
  -c:a copy \
  -f hls \
  -hls_time 8 \
  -hls_list_size 450 \
  -hls_flags delete_segments+append_list+temp_file \
  -hls_segment_type mpegts \
  -hls_segment_filename "${SEGMENT_PATTERN}" \
  "${PLAYLIST}" &
ffmpeg_pid=$!

wait "${ffmpeg_pid}"
