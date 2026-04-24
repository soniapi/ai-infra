FROM rust:1.80-bullseye as builder
WORKDIR /app
RUN apt-get update && apt-get install -y protobuf-compiler libpq-dev
COPY . .
RUN cargo build --release --bin grpc_schema_service

FROM debian:bullseye-slim
WORKDIR /app
RUN apt-get update && apt-get install -y libpq-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/grpc_schema_service /app/grpc_schema_service
CMD ["/app/grpc_schema_service"]
