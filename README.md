# ai_infra

`ai_infra` is a Rust-based project that serves as an infrastructure component for AI systems. It provides an independent context provider via a gRPC service for an LLM's Encoder block. The project leverages the **Diesel ORM** with **PostgreSQL** for database interactions and uses the **calamine** crate for processing Excel (`.xlsx`) files.

## Project Overview

- **gRPC Service**: Exposes a `ContextService` that returns database schema (`ColumnDefinition`), statistics (`ColumnStatistics`), and partition details (`PartitionInfo`). This service acts as an independent context provider, decoupling the Rust LLM from tight SQL/Postgres logic.
- **Data Ingestion**: A binary (`write_objects_from_xlsx`) processes Excel files and ingests data into the PostgreSQL database.
- **ORM**: Uses `diesel` to manage database schema and queries safely.
- **Protocol Buffers**: Uses `tonic` and `prost` to implement gRPC microservices.

## Prerequisites

Before building and running the project, ensure you have the required system dependencies installed:

```bash
sudo apt-get update
sudo apt-get install -y postgresql libpq-dev protobuf-compiler
sudo service postgresql start
```

You must also have Rust installed (version 1.85 or higher is required as the project uses Rust edition 2024).

## Database Setup

1. **Create the Database and User**:
   Set up a local test database and user in PostgreSQL. (Note: These commands should be run separately to avoid transaction block errors):

   ```bash
   sudo -u postgres psql -c "CREATE USER usr WITH PASSWORD 'pwd';"
   sudo -u postgres psql -c "CREATE DATABASE \"name-postgres\" OWNER usr;"
   ```

2. **Install Diesel CLI**:
   Install the Diesel CLI to manage database migrations:

   ```bash
   cargo install diesel_cli --no-default-features --features postgres
   ```

3. **Configure Environment Variables**:
   Add a `.env` file at the root of the project to allow Diesel to connect to your database. **Do not commit this file to version control.**

   ```env
   DATABASE_URL=postgres://usr:pwd@localhost:5432/name-postgres
   ```

4. **Run Database Migrations**:
   Set up the database schema using Diesel:

   ```bash
   DATABASE_URL=postgres://usr:pwd@localhost:5432/name-postgres diesel setup
   ```

## Running the Application

### 1. Data Ingestion
To run the primary script that processes data from Excel files and writes objects to the database:

```bash
cargo run --bin write_objects_from_xlsx
```

### 2. gRPC Service
To run the gRPC server locally:

```bash
cargo run --bin server
```
*Note: The service defaults to listening on port 8080. For local testing, ensure your client targets `0.0.0.0:8080`.*

## Testing

To run the project's test suite:

```bash
cargo test
```

When integration testing the gRPC server or data ingestion, the `.env` file must be present at the root of the workspace. Integration tests in `tests/integration_tests.rs` instantiate models from `src/models.rs`.

## Deployment

Automated deployments to **Google Cloud Run** are handled by a GitHub Actions workflow (`.github/workflows/deploy.yml`) triggered on pushes to the `master` branch.

- The gRPC service is deployed as a private, service-to-service API.
- The service exposes port `8080` and requires HTTP/2.
- Authentication for integration tests in CI is managed via GCP Identity Tokens. Service accounts executing source-based deployments require specific IAM roles (Storage Admin, Cloud Build Editor, Artifact Registry Writer, Service Account User, Cloud Run Admin, and Cloud Run Invoker).
- A `.dockerignore` file excluding the `target/` directory is present to prevent massive local artifact uploads to Cloud Build.

### Client Example on Google Cloud Run
`ai-infra` is available as a gRPC server service on Google Cloud. Below is sample code demonstrating how another client service running on Google Cloud can connect to `https://server-5u7ahgmduq-uc.a.run.app` and request database information securely.

The sample illustrates using `tonic::transport::ClientTlsConfig` and injecting a Google Cloud identity token via an interceptor to authenticate the request against the private API.

```rust
use tonic::{
    metadata::{AsciiMetadataValue, MetadataValue},
    transport::{Channel, ClientTlsConfig},
    Request,
};
use std::env;

// Important: Note that you would need to import ContextServiceClient and HypothesisContextRequest
// using a tonic::include_proto! macro that aligns with the ai_infra protobuf definition.

async fn connect_to_cloud_run() -> Result<(), Box<dyn std::error::Error>> {
    let cloud_run_url = "https://server-5u7ahgmduq-uc.a.run.app";

    // 1. Establish a secure channel with native TLS roots
    let tls_config = ClientTlsConfig::new().with_native_roots();
    let channel = Channel::from_static(cloud_run_url)
        .tls_config(tls_config)?
        .connect()
        .await?;

    // 2. Fetch the Bearer token (e.g. from gcloud or a GCP metadata service)
    let token = env::var("GCP_IDENTITY_TOKEN")
        .expect("GCP_IDENTITY_TOKEN environment variable not set");

    let bearer_token = format!("Bearer {}", token);
    let bearer_token_val: MetadataValue<tonic::metadata::Ascii> = bearer_token.parse()?;

    // 3. Create the client with an interceptor to inject the token
    // let mut client = ContextServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
    //     req.metadata_mut().insert("authorization", bearer_token_val.clone());
    //     Ok(req)
    // });

    // 4. Send the request
    // let request = tonic::Request::new(HypothesisContextRequest { ... });
    // let response = client.get_hypothesis_context(request).await?;
    // println!("Context Response: {:?}", response.into_inner());

    Ok(())
}
```

## License

This project is licensed under the Apache License v2.0. See the `LICENSE-ALv2.md` file for more details.
