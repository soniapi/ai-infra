# BUILD STAGE (using Debian 12 Bookworm)
FROM rust:1.85-slim-bookworm as builder
WORKDIR /app
RUN apt-get update && apt-get install -y libpq-dev
COPY . .
RUN cargo build --release --bin rest_api

# RUNTIME STAGE (MUST also use Debian 12 Bookworm)
FROM debian:bookworm-slim
WORKDIR /app
# Install necessary runtime libs for PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev ca-certificates libc6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/rest_api /app/rest_api
EXPOSE 8080
ENV PORT=8080
CMD ["/app/rest_api"]
