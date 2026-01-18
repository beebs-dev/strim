ARG RUNTIME_IMAGE=debian:bookworm-slim
ARG BASE_IMAGE=thavlik/strim-base:latest

FROM ${BASE_IMAGE} AS builder
COPY common/src common/src
COPY strim/src strim/src
COPY types/src types/src
WORKDIR /app/strim
RUN cargo build --release --bin strim

FROM ${RUNTIME_IMAGE}
COPY --from=builder /app/target/release/strim /usr/local/bin/strim
CMD ["strim"]