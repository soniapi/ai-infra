# BUILD STAGE (using Debian 12 Bookworm)
FROM rust:1.88-slim-bookworm as builder
WORKDIR /app
RUN apt-get update && apt-get install -y protobuf-compiler libpq-dev
COPY . .
RUN cargo build --release --bin server

# RUNTIME STAGE (MUST also use Debian 12 Bookworm)
FROM debian:bookworkom-slim
WORKDIR /app
# Install necessary runtime libs for Rust/gRPC
RUN apt-get update && apt-get install -y libpq-dev ca-certificates libc6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/server /app/server
EXPOSE 8080
CMD ["/app/server"]
