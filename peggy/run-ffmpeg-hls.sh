#!/usr/bin/env bash
set -euo pipefail

# ── Required env vars ────────────────────────────────────────────────────────────
: "${RTMP_URL:?RTMP_URL env var is required}"               # e.g. rtmp://rtmp-service/live/beebs

# Optional but you’re already passing them
S3_REGION="${S3_REGION:-}"

# HLS output directory (must be a shared volume)
HLS_DIR="${HLS_DIR:-/hls}"

mkdir -p "${HLS_DIR}"

#echo "run-ffmpeg-hls: RTMP_URL      = ${RTMP_URL}" >&2
#echo "run-ffmpeg-hls: HLS_DIR       = ${HLS_DIR}" >&2

# Playlist and segment names
PLAYLIST="${HLS_DIR}/index.m3u8"
SEGMENT_PATTERN="${HLS_DIR}/segment_%05d.ts"

#echo "run-ffmpeg-hls: writing playlist to ${PLAYLIST}" >&2
#echo "run-ffmpeg-hls: writing segments  to ${SEGMENT_PATTERN}" >&2

exec ffmpeg \
  -hide_banner \
  -loglevel info \
  -thread_queue_size 1024 \
  -i "${RTMP_URL}" \
  -c:v copy \
  -c:a copy \
  -f hls \
  -hls_time 8 \
  -hls_list_size 450 \
  -hls_flags delete_segments+append_list+temp_file \
  -hls_segment_type mpegts \
  -hls_segment_filename "${SEGMENT_PATTERN}" \
  "${PLAYLIST}" 2>&1 | ccze -A