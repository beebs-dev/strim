ARG RUNTIME_IMAGE=debian:bookworm-slim
ARG BASE_IMAGE=thavlik/strim-base:latest

FROM ${BASE_IMAGE} AS builder
COPY common/src common/src
COPY peggy/src peggy/src
WORKDIR /app/peggy
RUN cargo build --release --bin strim-peggy

FROM ${RUNTIME_IMAGE}
RUN apt-get update && apt-get install -y \
      ffmpeg \
      ca-certificates \
      ccze \
  && rm -rf /var/lib/apt/lists/*
COPY peggy/run-ffmpeg-hls.sh  /usr/local/bin/run-ffmpeg-hls.sh
RUN chmod +x /usr/local/bin/run-ffmpeg-hls.sh
COPY --from=builder /app/target/release/strim-peggy /usr/local/bin/strim-peggy
CMD ["strim-peggy"]
