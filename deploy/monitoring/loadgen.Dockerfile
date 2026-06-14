# Load generator for the monitoring demo: builds the weir-client `push_simple`
# example and runs it in a loop against the shared socket so the dashboard has
# live data. Not a production artifact.
FROM rust:1-slim-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo build --release --locked -p weir-client --example push_simple

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/examples/push_simple /usr/local/bin/push_simple
COPY deploy/monitoring/loadgen-loop.sh /usr/local/bin/loadgen-loop.sh
# chaos-probe.sh ships in the same image; the chaos profile's probe service runs
# it (as root) instead of loadgen-loop.sh by overriding the entrypoint.
COPY deploy/monitoring/chaos-probe.sh /usr/local/bin/chaos-probe.sh
RUN chmod +x /usr/local/bin/loadgen-loop.sh /usr/local/bin/chaos-probe.sh
ENTRYPOINT ["/usr/local/bin/loadgen-loop.sh"]
