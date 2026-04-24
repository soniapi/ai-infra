use ai_infra::schema_service::{
    schema_service_server::{SchemaService, SchemaServiceServer},
    GetSchemaRequest, GetSchemaResponse,
};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Default)]
pub struct MySchemaService {}

#[tonic::async_trait]
impl SchemaService for MySchemaService {
    async fn get_schema(
        &self,
        _request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let schema_ddl = include_str!("../schema.rs");
        Ok(Response::new(GetSchemaResponse {
            schema: schema_ddl.to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let schema_service = MySchemaService::default();

    println!("Schema service listening on {}", addr);

    Server::builder()
        .add_service(SchemaServiceServer::new(schema_service))
        .serve(addr)
        .await?;

    Ok(())
}
