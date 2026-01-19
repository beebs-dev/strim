#!/usr/bin/env bash
set -euo pipefail

RTMP_URLS_FILE="${RTMP_URLS_FILE:-/etc/rtmp_urls}"

HLS_DIR="${HLS_DIR:-/hls}"
mkdir -p "${HLS_DIR}"

PLAYLIST="${HLS_DIR}/index.m3u8"
SEGMENT_PATTERN="${HLS_DIR}/segment_%05d.ts"

# Switch cadence
SWITCH_EVERY="${SWITCH_EVERY:-5}"

# Refresh behavior
POLL_REFRESH_SECONDS="${POLL_REFRESH_SECONDS:-10}"
NO_STREAMS_SLEEP="${NO_STREAMS_SLEEP:-5}"

# Probe timeout (microseconds)
PROBE_TIMEOUT_US="${PROBE_TIMEOUT_US:-5000000}"

# Pre-generate this much schedule (seconds). Default: 24h.
SENDCMD_DURATION="${SENDCMD_DURATION:-86400}"
SENDCMD_FILE="${SENDCMD_FILE:-/tmp/jumbotron_cmds.txt}"

# Encoding (recommended to re-encode for clean cuts)
VIDEO_PRESET="${VIDEO_PRESET:-veryfast}"
VIDEO_CRF="${VIDEO_CRF:-23}"
AUDIO_BITRATE="${AUDIO_BITRATE:-128k}"

# If you know FPS, set it; otherwise leave blank.
# If set, we also set GOP = FPS*SWITCH_EVERY for perfect boundaries.
FPS="${FPS:-}"  # e.g. 30

# HLS settings
HLS_TIME="${HLS_TIME:-2}"          # recommend <= SWITCH_EVERY for visible switching
HLS_LIST_SIZE="${HLS_LIST_SIZE:-450}"

# Logging / debugging
USE_CCZE="${USE_CCZE:-1}"          # set 0 to disable ccze piping
FFMPEG_LOGLEVEL="${FFMPEG_LOGLEVEL:-info}"

ffmpeg_pid=""
last_good_urls_hash=""

cleanup() {
  if [[ -n "${ffmpeg_pid}" ]] && kill -0 "${ffmpeg_pid}" 2>/dev/null; then
    kill "${ffmpeg_pid}" 2>/dev/null || true
    wait "${ffmpeg_pid}" 2>/dev/null || true
  fi
}

terminate() {
  echo "Received termination signal, shutting down..." >&2
  cleanup
  exit 0
}

trap terminate TERM INT
trap cleanup EXIT

probe_url() {
  local url="$1"
  if ! command -v ffprobe >/dev/null 2>&1; then
    echo "WARN: ffprobe not found; treating as valid: ${url}" >&2
    return 0
  fi

  ffprobe \
    -v error \
    -rw_timeout "${PROBE_TIMEOUT_US}" \
    -select_streams v:0 \
    -show_entries stream=index \
    -of csv=p=0 \
    "${url}" >/dev/null 2>&1
}

read_urls_raw() {
  [[ -f "${RTMP_URLS_FILE}" ]] || return 1
  mapfile -t URLS < <(
    sed -e 's/[[:space:]]*$//' -e 's/^[[:space:]]*//' \
        -e 's/#.*$//' \
        "${RTMP_URLS_FILE}" | awk 'NF'
  )
  (( ${#URLS[@]} > 0 ))
}

filter_valid_urls() {
  local -a good=()
  local u
  for u in "${URLS[@]}"; do
    if probe_url "${u}"; then
      good+=("${u}")
    else
      echo "WARN: RTMP URL failed probe, skipping: ${u}" >&2
    fi
  done
  URLS=("${good[@]}")
}

urls_hash() {
  # stable hash of the current URL set
  printf '%s\n' "$@" | sha256sum | awk '{print $1}'
}

generate_sendcmd() {
  local n="$1"
  : > "${SENDCMD_FILE}"

  local t=0
  local last=-1
  while (( t <= SENDCMD_DURATION )); do
    local idx=$(( RANDOM % n ))
    if (( n > 1 )); then
      while (( idx == last )); do
        idx=$(( RANDOM % n ))
      done
    fi
    last="$idx"

    # Minimal syntax that works well across versions:
    # <time> <target> <command> <arg>
    printf "%d.0 streamselect map %d\n" "$t" "$idx" >> "${SENDCMD_FILE}"
    printf "%d.0 astreamselect map %d\n" "$t" "$idx" >> "${SENDCMD_FILE}"

    t=$(( t + SWITCH_EVERY ))
  done
}

build_filtergraph() {
  local n="$1"

  local v_inputs=""
  local a_inputs=""
  local i
  for ((i=0; i<n; i++)); do
    v_inputs+="[${i}:v]"
    a_inputs+="[${i}:a]"
  done

  # IMPORTANT:
  # - sendcmd is video-chain; asendcmd is audio-chain
  # - put them immediately before the corresponding *select filter
  FILTER_COMPLEX="\
${v_inputs}sendcmd=f=${SENDCMD_FILE},streamselect=inputs=${n}:map=0,setpts=N/FRAME_RATE/TB[v];\
${a_inputs}asendcmd=f=${SENDCMD_FILE},astreamselect=inputs=${n}:map=0,asetpts=N/SR/TB[a]"
}

run_ffmpeg_with_urls() {
  local -a urls=("$@")
  local n="${#urls[@]}"

  generate_sendcmd "${n}"
  build_filtergraph "${n}"

  local -a INPUT_ARGS=()
  local u
  for u in "${urls[@]}"; do
    # These help RTMP flakiness; adjust if needed:
    INPUT_ARGS+=(
      -thread_queue_size 1024
      -rw_timeout "${PROBE_TIMEOUT_US}"
      -fflags +genpts
      -i "${u}"
    )
  done

  echo "Using ${n} RTMP input(s):" >&2
  printf '  - %s\n' "${urls[@]}" >&2
  head -n 6 "${SENDCMD_FILE}" >&2

  # Keyframe / GOP alignment
  local -a GOP_ARGS=()
  if [[ -n "${FPS}" ]]; then
    # GOP = FPS * SWITCH_EVERY
    GOP_ARGS+=( -r "${FPS}" -g "$(( FPS * SWITCH_EVERY ))" -keyint_min "$(( FPS * SWITCH_EVERY ))" )
  fi

  # Run ffmpeg
  set +e
  if [[ "${USE_CCZE}" == "1" ]] && command -v ccze >/dev/null 2>&1; then
    ffmpeg \
      -hide_banner \
      -loglevel "${FFMPEG_LOGLEVEL}" \
      "${INPUT_ARGS[@]}" \
      -filter_complex "${FILTER_COMPLEX}" \
      -map "[v]" -map "[a]" \
      -c:v libx264 -preset "${VIDEO_PRESET}" -crf "${VIDEO_CRF}" -pix_fmt yuv420p \
      -tune zerolatency \
      "${GOP_ARGS[@]}" \
      -sc_threshold 0 \
      -force_key_frames "expr:gte(t,n_forced*${SWITCH_EVERY})" \
      -c:a aac -b:a "${AUDIO_BITRATE}" \
      -f hls \
      -hls_time "${HLS_TIME}" \
      -hls_list_size "${HLS_LIST_SIZE}" \
      -hls_flags delete_segments+append_list+temp_file \
      -hls_segment_type mpegts \
      -hls_segment_filename "${SEGMENT_PATTERN}" \
      "${PLAYLIST}" 2>&1 | ccze -A &
  else
    ffmpeg \
      -hide_banner \
      -loglevel "${FFMPEG_LOGLEVEL}" \
      "${INPUT_ARGS[@]}" \
      -filter_complex "${FILTER_COMPLEX}" \
      -map "[v]" -map "[a]" \
      -c:v libx264 -preset "${VIDEO_PRESET}" -crf "${VIDEO_CRF}" -pix_fmt yuv420p \
      -tune zerolatency \
      "${GOP_ARGS[@]}" \
      -sc_threshold 0 \
      -force_key_frames "expr:gte(t,n_forced*${SWITCH_EVERY})" \
      -c:a aac -b:a "${AUDIO_BITRATE}" \
      -f hls \
      -hls_time "${HLS_TIME}" \
      -hls_list_size "${HLS_LIST_SIZE}" \
      -hls_flags delete_segments+append_list+temp_file \
      -hls_segment_type mpegts \
      -hls_segment_filename "${SEGMENT_PATTERN}" \
      "${PLAYLIST}" &
  fi

  ffmpeg_pid=$!
  wait "${ffmpeg_pid}"
  local rc=$?
  ffmpeg_pid=""
  set -e
  return "${rc}"
}

# Build the valid URL set (blocking until >=1 valid)
get_valid_urls_blocking() {
  while true; do
    if ! read_urls_raw; then
      echo "ERROR: Cannot read ${RTMP_URLS_FILE}; sleeping ${NO_STREAMS_SLEEP}s..." >&2
      sleep "${NO_STREAMS_SLEEP}"
      continue
    fi

    filter_valid_urls

    if (( ${#URLS[@]} == 0 )); then
      echo "ERROR: No valid RTMP URLs; sleeping ${NO_STREAMS_SLEEP}s..." >&2
      sleep "${NO_STREAMS_SLEEP}"
      continue
    fi

    return 0
  done
}

have_inotify() { command -v inotifywait >/dev/null 2>&1; }

wait_for_urls_change() {
  if have_inotify; then
    inotifywait -q -e close_write,move,create,delete "${RTMP_URLS_FILE}" >/dev/null 2>&1 || true
  else
    # polling fallback
    local last=""
    last="$(sha256sum "${RTMP_URLS_FILE}" 2>/dev/null | awk '{print $1}' || true)"
    while true; do
      sleep "${POLL_REFRESH_SECONDS}"
      local now=""
      now="$(sha256sum "${RTMP_URLS_FILE}" 2>/dev/null | awk '{print $1}' || true)"
      if [[ -n "${now}" && "${now}" != "${last}" ]]; then
        break
      fi
    done
  fi
}

main() {
  echo "Watching ${RTMP_URLS_FILE} for changes..." >&2

  while true; do
    # Get current valid set (blocking until >=1)
    get_valid_urls_blocking
    local current_hash
    current_hash="$(urls_hash "${URLS[@]}")"

    # If we already have a running "last good", avoid restart if effective set is same.
    if [[ -n "${last_good_urls_hash}" && "${current_hash}" == "${last_good_urls_hash}" ]]; then
      echo "Valid URL set unchanged; not restarting. Waiting for change..." >&2
      wait_for_urls_change
      continue
    fi

    # Update last-good and run ffmpeg
    last_good_urls_hash="${current_hash}"
    cleanup
    run_ffmpeg_with_urls "${URLS[@]}" || true

    # ffmpeg exited: wait for url file to change before re-evaluating
    echo "ffmpeg exited; waiting for ${RTMP_URLS_FILE} to change before restart..." >&2
    wait_for_urls_change
  done
}

main "$@"
