use ai_infra::schema_service::{
    context_service_server::{ContextService, ContextServiceServer},
    ColumnDefinition, HypothesisContextRequest, HypothesisContextResponse,
};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Default)]
pub struct MyContextService {}

#[tonic::async_trait]
impl ContextService for MyContextService {
    async fn get_hypothesis_context(
        &self,
        _request: Request<HypothesisContextRequest>,
    ) -> Result<Response<HypothesisContextResponse>, Status> {
        let columns = vec![
            ColumnDefinition {
                column_name: "id".to_string(),
                data_type: "Int4".to_string(),
                is_partition_key: false,
            },
            ColumnDefinition {
                column_name: "d".to_string(),
                data_type: "Timestamp".to_string(),
                is_partition_key: false,
            },
            ColumnDefinition {
                column_name: "t".to_string(),
                data_type: "Text".to_string(),
                is_partition_key: false,
            },
            ColumnDefinition {
                column_name: "p".to_string(),
                data_type: "Float4".to_string(),
                is_partition_key: false,
            },
            ColumnDefinition {
                column_name: "s".to_string(),
                data_type: "Float4".to_string(),
                is_partition_key: true,
            },
            ColumnDefinition {
                column_name: "c".to_string(),
                data_type: "Float4".to_string(),
                is_partition_key: false,
            },
        ];

        Ok(Response::new(HypothesisContextResponse {
            schema: columns,
            stats: vec![],
            active_partitions: vec![],
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port).parse()?;
    let context_service = MyContextService::default();

    println!("Context service listening on {}", addr);

    Server::builder()
        .add_service(ContextServiceServer::new(context_service))
        .serve(addr)
        .await?;

    Ok(())
}
