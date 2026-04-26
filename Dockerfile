FROM rust:1.88-slim as builder
WORKDIR /app
RUN apt-get update && apt-get install -y protobuf-compiler libpq-dev
COPY . .
RUN cargo build --release --bin server

FROM debian:bullseye-slim
WORKDIR /app
RUN apt-get update && apt-get install -y libpq-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/server /app/server
EXPOSE 8080
CMD ["/app/server"]
