ARG RUNTIME_IMAGE=debian:bookworm-slim
ARG BASE_IMAGE=thavlik/strim-base:latest

FROM ${BASE_IMAGE} AS builder
COPY common/src common/src
COPY types/src types/src
COPY operator/src operator/src
WORKDIR /app/operator
RUN cargo build --release --bin strim-operator

FROM ${RUNTIME_IMAGE}
COPY --from=builder /app/target/release/strim-operator /usr/local/bin/strim-operator
CMD ["strim-operator"]
