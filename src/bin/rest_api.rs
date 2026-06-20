use axum::{
    Router,
    extract::{DefaultBodyLimit, Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use std::env;
use std::io::Cursor;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use calamine::open_workbook_auto_from_rs;

use ai_infra::models::{NewObject, NewObjectS};
use ai_infra::{create_objects, create_objects_s, establish_connection, process_workbook};
use axum::Json;
use axum::extract::Query;
use diesel::prelude::*;
use diesel::sql_query;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use diesel_migrations::FileBasedMigrations;
use serde::{Deserialize, Serialize};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[derive(Deserialize)]
struct MigrationsParams {
    clear: Option<bool>,
}

#[derive(Deserialize)]
struct InfoParams {
    db_names: Option<String>,
    db: Option<String>,
    table: Option<String>,
    n: Option<i32>,
}

#[derive(QueryableByName, Serialize)]
struct DbName {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
}

#[derive(QueryableByName, Serialize)]
struct TableInfo {
    #[diesel(sql_type = diesel::sql_types::Text)]
    table_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    table_type: String,
}

#[derive(QueryableByName)]
struct RowCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Serialize)]
struct CountResult {
    count: i64,
}

#[derive(QueryableByName)]
struct RowData {
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    json_data: serde_json::Value,
}

#[derive(diesel::query_builder::QueryId, QueryableByName)]
struct DdlResult {
    #[diesel(sql_type = diesel::sql_types::Text)]
    ddl: String,
}

#[derive(Serialize)]
struct MigrationStatus {
    name: String,
    version: String,
    applied: bool,
}

fn establish_connection_to(db_name: Option<&str>) -> Result<PgConnection, String> {
    let database_url =
        env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_string())?;
    let url = if let Some(db) = db_name {
        let parts: Vec<&str> = database_url.rsplitn(2, '/').collect();
        if parts.len() == 2 {
            format!("{}/{}", parts[1], db)
        } else {
            database_url
        }
    } else {
        database_url
    };
    PgConnection::establish(&url).map_err(|e| format!("Error connecting to database: {}", e))
}

async fn info_handler(Query(params): Query<InfoParams>) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        if params.db_names.is_some() {
            let mut conn = establish_connection_to(None)?;
            let databases = sql_query("SELECT datname as name FROM pg_database WHERE datistemplate = false;")
                .load::<DbName>(&mut conn)
                .map_err(|e| format!("Query error: {}", e))?;
            return Ok(serde_json::to_value(databases).unwrap());
        }

        if let Some(table) = params.table {
            let mut conn = establish_connection_to(params.db.as_deref())?;
            if let Some(n) = params.n {
                let ddl: String = sql_query("SELECT format('SELECT to_jsonb(t.*) as json_data FROM %I t LIMIT %s', $1, $2) as ddl")
                    .bind::<diesel::sql_types::Text, _>(&table)
                    .bind::<diesel::sql_types::Text, _>(&n.to_string())
                    .load::<DdlResult>(&mut conn)
                    .map_err(|e| format!("Failed to construct SQL: {}", e))?
                    .pop().ok_or("Failed to get DDL")?.ddl;

                let rows = sql_query(ddl)
                    .load::<RowData>(&mut conn)
                    .map_err(|e| format!("Query error: {}", e))?;

                let json_rows: Vec<serde_json::Value> = rows.into_iter().map(|r| r.json_data).collect();
                return Ok(serde_json::to_value(json_rows).unwrap());
            } else {
                let ddl: String = sql_query("SELECT format('SELECT count(*) as count FROM %I', $1) as ddl")
                    .bind::<diesel::sql_types::Text, _>(&table)
                    .load::<DdlResult>(&mut conn)
                    .map_err(|e| format!("Failed to construct SQL: {}", e))?
                    .pop().ok_or("Failed to get DDL")?.ddl;

                let count = sql_query(ddl)
                    .load::<RowCount>(&mut conn)
                    .map_err(|e| format!("Query error: {}", e))?
                    .pop().ok_or("Failed to get count")?.count;

                return Ok(serde_json::to_value(CountResult { count }).unwrap());
            }
        }

        if let Some(db) = params.db {
            let mut conn = establish_connection_to(Some(&db))?;
            let tables = sql_query(
                "SELECT c.relname as table_name,
                        CASE
                            WHEN c.relkind = 'p' THEN 'partition table'
                            WHEN c.relkind = 'r' AND c.relispartition THEN 'partition'
                            WHEN c.relkind = 'r' AND NOT c.relispartition THEN 'non-partition table'
                            ELSE 'other'
                        END as table_type
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY c.relname;"
            )
            .load::<TableInfo>(&mut conn)
            .map_err(|e| format!("Query error: {}", e))?;
            return Ok(serde_json::to_value(tables).unwrap());
        }

        Err("Invalid parameters".to_string())
    }).await;

    match result {
        Ok(Ok(json)) => (StatusCode::OK, Json(json)).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Task execution failed: {}", e),
        )
            .into_response(),
    }
}

async fn migrations_handler(params: Option<axum::extract::Query<MigrationsParams>>) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let mut conn = establish_connection_to(None)?;

        let migrations = FileBasedMigrations::from_path("migrations")
            .map_err(|e| format!("Error loading migrations: {}", e))?;

        if let Some(axum::extract::Query(p)) = params {
            if p.clear.unwrap_or(false) {
                conn.revert_last_migration(migrations.clone())
                    .map_err(|e| format!("Error reverting migration: {}", e))?;
            }
        }

        let applied = conn
            .applied_migrations()
            .map_err(|e| format!("Error getting applied migrations: {}", e))?;
        let pending = conn
            .pending_migrations(migrations.clone())
            .map_err(|e| format!("Error getting pending migrations: {}", e))?;

        let mut statuses = Vec::new();

        let all_embedded = diesel::migration::MigrationSource::<diesel::pg::Pg>::migrations(&migrations.clone())
            .map_err(|e| format!("Error getting embedded migrations: {}", e))?;

        for m in applied {
            let version_str = m.to_string();
            let mut name_str = version_str.clone();

            for emb in &all_embedded {
                let emb_v = emb.name().version().to_string();
                if emb_v == version_str || emb_v.replace("-", "") == version_str.replace("-", "") {
                    name_str = emb.name().to_string();
                    break;
                }
            }

            statuses.push(MigrationStatus {
                name: name_str,
                version: version_str,
                applied: true,
            });
        }

        for m in pending {
            let version = m.name().version().to_string();
            statuses.push(MigrationStatus {
                name: m.name().to_string(),
                version,
                applied: false,
            });
        }

        // Sort migrations by version
        statuses.sort_by(|a, b| a.version.cmp(&b.version));

        Ok(serde_json::to_value(statuses).unwrap())
    })
    .await;

    match result {
        Ok(Ok(json)) => (StatusCode::OK, Json(json)).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Task execution failed: {}", e),
        )
            .into_response(),
    }
}


#[derive(QueryableByName, Serialize)]
struct ColumnInfo {
    #[diesel(sql_type = diesel::sql_types::Text)]
    column_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    data_type: String,
}

#[derive(QueryableByName)]
struct StringResult {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

#[derive(Serialize)]
struct PartitionSchemaResponse {
    table_name: String,
    columns: Vec<ColumnInfo>,
    primary_key: String,
    partition_strategy: String,
    created_at: String,
}

#[derive(Deserialize)]
struct SplitParams {
    #[serde(rename = "type")]
    partition_type: String,
    cutoff: f32,
}

async fn split_handler(params: axum::extract::Query<SplitParams>) -> impl IntoResponse {
    let partition_type = params.partition_type.clone();
    let cutoff = params.cutoff;

    let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let mut conn = establish_connection_to(None)?;

        let (up_sql, down_sql) = ai_infra::generate_partition_migration_sql(&mut conn, &partition_type, cutoff)?;

        let now = chrono::Utc::now();
        let dir_name = format!("migrations/{}_split_objects_{}", now.format("%Y%m%d%H%M%S"), partition_type);

        std::fs::create_dir_all(&dir_name)
            .map_err(|e| format!("Failed to create migration directory: {}", e))?;

        std::fs::write(format!("{}/up.sql", dir_name), up_sql)
            .map_err(|e| format!("Failed to write up.sql: {}", e))?;

        std::fs::write(format!("{}/down.sql", dir_name), down_sql)
            .map_err(|e| format!("Failed to write down.sql: {}", e))?;

        let migrations = diesel_migrations::FileBasedMigrations::from_path("migrations")
            .map_err(|e| format!("Error loading migrations: {}", e))?;

        conn.run_pending_migrations(migrations.clone())
            .map_err(|e| format!("Failed to run dynamically generated migration: {}", e))?;

        Ok(())
    })
    .await
    .unwrap();

    match result {
        Ok(_) => (axum::http::StatusCode::OK, "Partitions split successfully".to_string()),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error splitting partitions: {}", e),
        ),
    }
}

#[derive(Deserialize)]
struct PartitionParams {
    #[serde(rename = "type")]
    partition_type: String,
}

async fn partition_handler(params: axum::extract::Query<PartitionParams>) -> impl IntoResponse {
    let partition_type = params.partition_type.clone();

    if partition_type == "s" {
        let result = tokio::task::spawn_blocking(move || -> Result<PartitionSchemaResponse, String> {
            let mut conn = establish_connection_to(None)?;

            #[derive(diesel::query_builder::QueryId, diesel::QueryableByName)]
            struct ExistsResult {
                #[diesel(sql_type = diesel::sql_types::Bool)]
                exists: bool,
            }

            let query = diesel::sql_query(
                "SELECT EXISTS (
                    SELECT FROM pg_tables
                    WHERE schemaname = 'public'
                    AND tablename = 'objects_s'
                ) as exists",
            );

            let mut exists = false;
            if let Ok(mut results) = query.load::<ExistsResult>(&mut conn) {
                if let Some(res) = results.pop() {
                    exists = res.exists;
                }
            }

            if !exists {
                let up_sql = "CREATE TABLE objects_s (\n    id SERIAL,\n    d TIMESTAMP NOT NULL,\n    t TEXT NOT NULL,\n    p REAL NOT NULL,\n    s REAL NOT NULL,\n    c REAL NOT NULL,\n    PRIMARY KEY (id, s)\n) PARTITION BY RANGE (s);";
                let down_sql = "DROP TABLE objects_s";

                let now = chrono::Utc::now();
                let _version_str = now.format("%Y%m%d%H%M%S").to_string();
                let dir_name = format!("migrations/{}_create_objects_s", now.format("%Y%m%d%H%M%S"));

                std::fs::create_dir_all(&dir_name)
                    .map_err(|e| format!("Failed to create migration directory: {}", e))?;

                std::fs::write(format!("{}/up.sql", dir_name), up_sql)
                    .map_err(|e| format!("Failed to write up.sql: {}", e))?;

                std::fs::write(format!("{}/down.sql", dir_name), down_sql)
                    .map_err(|e| format!("Failed to write down.sql: {}", e))?;

                let migrations = diesel_migrations::FileBasedMigrations::from_path("migrations")
                    .map_err(|e| format!("Error loading migrations: {}", e))?;

                conn.run_pending_migrations(migrations.clone())
                    .map_err(|e| format!("Failed to run dynamically generated migration: {}", e))?;
            }

            let columns = diesel::sql_query("SELECT column_name::text, data_type::text FROM information_schema.columns WHERE table_name = 'objects_s' ORDER BY ordinal_position")
                .load::<ColumnInfo>(&mut conn)
                .map_err(|e| format!("Failed to retrieve schema: {}", e))?;

            let primary_key = diesel::sql_query("SELECT indexdef::text as value FROM pg_indexes WHERE tablename = 'objects_s' AND indexname = 'objects_s_pkey'")
                .load::<StringResult>(&mut conn)
                .map_err(|e| format!("Failed to retrieve primary key: {}", e))?
                .into_iter()
                .next()
                .map(|r| r.value)
                .unwrap_or_else(|| "Unknown".to_string());

            let partition_strategy = diesel::sql_query("SELECT pg_get_partkeydef('objects_s'::regclass)::text as value")
                .load::<StringResult>(&mut conn)
                .map_err(|e| format!("Failed to retrieve partition strategy: {}", e))?
                .into_iter()
                .next()
                .map(|r| r.value)
                .unwrap_or_else(|| "Unknown".to_string());

            let created_at = diesel::sql_query("SELECT run_on::text as value FROM __diesel_schema_migrations ORDER BY run_on DESC LIMIT 1")
                .load::<StringResult>(&mut conn)
                .map_err(|e| format!("Failed to retrieve creation timestamp: {}", e))?
                .into_iter()
                .next()
                .map(|r| r.value)
                .unwrap_or_else(|| "Unknown".to_string());

            Ok(PartitionSchemaResponse {
                table_name: "objects_s".to_string(),
                columns,
                primary_key,
                partition_strategy,
                created_at,
            })
        })
        .await;

        match result {
            Ok(Ok(response)) => (StatusCode::OK, Json(response)).into_response(),
            Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)).into_response(),
        }
    } else {
        (StatusCode::BAD_REQUEST, "Invalid partition type").into_response()
    }
}

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

        let mut excel = open_workbook_auto_from_rs(cursor)
            .map_err(|e| format!("Error parsing workbook: {}", e))?;

        let connection = &mut establish_connection();

        let is_partition_s = partition_type.as_deref() == Some("s");
        let mut objects = Vec::with_capacity(1000);
        let mut objects_s = Vec::with_capacity(1000);
        let mut total_inserted = 0;

        process_workbook(
            &mut excel,
            &tab_name,
            row_limit,
            |d, t_val, p_val, s_val| {
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
            },
        );

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
    })
    .await;

    match result {
        Ok(Ok(count)) => (
            StatusCode::OK,
            format!(
                "File received and processed successfully. Inserted {} rows.",
                count
            ),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Task execution failed: {}", e),
        )
            .into_response(),
    }
}

#[tokio::main]
async fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "8081".to_string());
    let port = port.trim();
    let addr = format!("0.0.0.0:{}", port);

    let app = Router::new()
        .route("/upload", post(upload_handler))
        .route("/partition", axum::routing::get(partition_handler))
        .route("/split", axum::routing::post(split_handler))
        .route("/info", axum::routing::get(info_handler))
        .route("/migrations", axum::routing::get(migrations_handler))
        .layer(DefaultBodyLimit::disable())
        .layer(CorsLayer::permissive());

    println!("REST API server listening on {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
