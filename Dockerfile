# BUILD STAGE
FROM rust:1.85-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y libpq-dev pkg-config

WORKDIR /usr/src/app
COPY . .

# Build the rest API
RUN cargo build --release --bin rest_api

# RUNTIME STAGE
FROM debian:bookworm-slim

# Install necessary runtime libs for PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev ca-certificates libc6 && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY --from=builder /usr/src/app/target/release/rest_api /usr/local/bin/rest_api

EXPOSE 8080
ENV PORT=8080

CMD ["rest_api"]
