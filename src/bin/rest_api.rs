use axum::{
    extract::Multipart,
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

    while let Some(field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();

        if name == "f" {
            file_bytes = Some(field.bytes().await.unwrap().to_vec());
        } else if name == "t" {
            tab_name = Some(String::from_utf8_lossy(&field.bytes().await.unwrap()).to_string());
        } else if name == "p" {
            let p_val = String::from_utf8_lossy(&field.bytes().await.unwrap()).to_string();
            if !p_val.is_empty() {
                partition_type = Some(p_val);
            }
        } else if name == "r" {
            let r_val = String::from_utf8_lossy(&field.bytes().await.unwrap()).to_string();
            if !r_val.is_empty() {
                row_limit = r_val.parse::<i32>().ok();
            }
        }
    }

    if file_bytes.is_none() || tab_name.is_none() {
        return (StatusCode::BAD_REQUEST, "Missing required fields (f, t)").into_response();
    }

    let file_bytes = file_bytes.unwrap();
    let tab_name = tab_name.unwrap();

    let cursor = Cursor::new(file_bytes);

    let mut excel = match open_workbook_auto_from_rs(cursor) {
        Ok(workbook) => workbook,
        Err(e) => {
            let err_msg = format!("Error parsing workbook: {}", e);
            return (StatusCode::BAD_REQUEST, err_msg).into_response();
        }
    };

    let connection = &mut establish_connection();

    let is_partition_s = partition_type.as_deref() == Some("s");
    let mut objects = Vec::new();
    let mut objects_s = Vec::new();

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
                let _ = create_objects_s(connection, &objects_s);
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
                let _ = create_objects(connection, &objects);
                objects.clear();
            }
        }
    });

    if !objects_s.is_empty() {
        let _ = create_objects_s(connection, &objects_s);
    }
    if !objects.is_empty() {
        let _ = create_objects(connection, &objects);
    }

    (StatusCode::OK, "File received and processed successfully").into_response()
}

#[tokio::main]
async fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "8081".to_string());
    let port = port.trim();
    let addr = format!("0.0.0.0:{}", port);

    let app = Router::new()
        .route("/upload", post(upload_handler))
        .layer(CorsLayer::permissive());

    println!("REST API server listening on {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
