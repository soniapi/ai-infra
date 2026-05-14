use axum::{
    extract::{Multipart, DefaultBodyLimit},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use std::env;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use std::io::Cursor;

use calamine::open_workbook_auto_from_rs;

use ai_infra::models::{NewObject, NewObjectS};
use ai_infra::{create_objects, create_objects_s, establish_connection, process_workbook};

async fn upload_handler(mut multipart: Multipart) -> impl IntoResponse {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut tab_name: Option<String> = None;
    let mut partition_type: Option<String> = None;
    let mut row_limit: Option<i32> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();

        if name == "f" {
            if let Ok(bytes) = field.bytes().await {
                file_bytes = Some(bytes.to_vec());
            }
        } else if name == "t" {
            if let Ok(bytes) = field.bytes().await {
                tab_name = Some(String::from_utf8_lossy(&bytes).to_string());
            }
        } else if name == "p" {
            if let Ok(bytes) = field.bytes().await {
                let p_val = String::from_utf8_lossy(&bytes).to_string();
                if !p_val.is_empty() {
                    partition_type = Some(p_val);
                }
            }
        } else if name == "r" {
            if let Ok(bytes) = field.bytes().await {
                let r_val = String::from_utf8_lossy(&bytes).to_string();
                if !r_val.is_empty() {
                    row_limit = r_val.parse::<i32>().ok();
                }
            }
        }
    }

    if file_bytes.is_none() || tab_name.is_none() {
        return (StatusCode::BAD_REQUEST, "Missing required fields (f, t)").into_response();
    }

    let file_bytes = file_bytes.unwrap();
    let tab_name = tab_name.unwrap();

    // Move CPU-heavy parsing and blocking DB calls to spawn_blocking
    let result = tokio::task::spawn_blocking(move || -> Result<usize, String> {
        let cursor = Cursor::new(file_bytes);

        let mut excel = open_workbook_auto_from_rs(cursor).map_err(|e| format!("Error parsing workbook: {}", e))?;

        let connection = &mut establish_connection();

        let is_partition_s = partition_type.as_deref() == Some("s");
        let mut objects = Vec::with_capacity(1000);
        let mut objects_s = Vec::with_capacity(1000);
        let mut total_inserted = 0;

        process_workbook(&mut excel, &tab_name, row_limit, |d, t_val, p_val, s_val| {
            if is_partition_s {
                objects_s.push(NewObjectS {
                    d: *d,
                    t: t_val.to_string(),
                    p: p_val,
                    s: s_val,
                    c: 0.0,
                });
                if objects_s.len() >= 1000 {
                    if let Ok(count) = create_objects_s(connection, &objects_s) {
                        total_inserted += count;
                    }
                    objects_s.clear();
                }
            } else {
                objects.push(NewObject {
                    d: *d,
                    t: t_val.to_string(),
                    p: p_val,
                    s: s_val,
                    c: 0.0,
                });
                if objects.len() >= 1000 {
                    if let Ok(count) = create_objects(connection, &objects) {
                        total_inserted += count;
                    }
                    objects.clear();
                }
            }
        });

        if !objects_s.is_empty() {
            if let Ok(count) = create_objects_s(connection, &objects_s) {
                total_inserted += count;
            }
        }
        if !objects.is_empty() {
            if let Ok(count) = create_objects(connection, &objects) {
                total_inserted += count;
            }
        }

        Ok(total_inserted)
    }).await;

    match result {
        Ok(Ok(count)) => (StatusCode::OK, format!("File received and processed successfully. Inserted {} rows.", count)).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Task execution failed: {}", e)).into_response(),
    }
}

#[tokio::main]
async fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "8081".to_string());
    let port = port.trim();
    let addr = format!("0.0.0.0:{}", port);

    let app = Router::new()
        .route("/upload", post(upload_handler))
        .layer(DefaultBodyLimit::disable())
        .layer(CorsLayer::permissive());

    println!("REST API server listening on {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
