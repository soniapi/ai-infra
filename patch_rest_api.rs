use std::fs;

fn main() {
    let filename = "src/bin/rest_api.rs";
    let content = fs::read_to_string(filename).unwrap();

    // Add use diesel::migration::MigrationSource;
    let import_target = "use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};\nuse serde::{Deserialize, Serialize};";
    let import_replacement = "use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};\nuse diesel::migration::MigrationSource;\nuse serde::{Deserialize, Serialize};";

    let content = content.replace(import_target, import_replacement);

    let query_param = r#"#[derive(Deserialize)]
struct InfoParams {"#;
    let query_param_replacement = r#"#[derive(Deserialize)]
struct MigrationsParams {
    clear: Option<bool>,
}

#[derive(Deserialize)]
struct InfoParams {"#;
    let content = content.replace(query_param, query_param_replacement);

    let handler_target = r#"async fn migrations_handler() -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let mut conn = establish_connection_to(None)?;
        let applied = conn
            .applied_migrations()
            .map_err(|e| format!("Error getting applied migrations: {}", e))?;
        let pending = conn
            .pending_migrations(MIGRATIONS)
            .map_err(|e| format!("Error getting pending migrations: {}", e))?;

        let mut statuses = Vec::new();

        for m in applied {
            statuses.push(MigrationStatus {
                name: m.to_string(),
                version: m.to_string(),
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
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Task execution failed: {}", e)).into_response(),
    }
}"#;

    let handler_replacement = r#"async fn migrations_handler(Query(params): Query<MigrationsParams>) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let mut conn = establish_connection_to(None)?;

        if params.clear.unwrap_or(false) {
            conn.revert_last_migration(MIGRATIONS)
                .map_err(|e| format!("Error reverting migration: {}", e))?;
        }

        let applied = conn
            .applied_migrations()
            .map_err(|e| format!("Error getting applied migrations: {}", e))?;
        let pending = conn
            .pending_migrations(MIGRATIONS)
            .map_err(|e| format!("Error getting pending migrations: {}", e))?;

        let mut statuses = Vec::new();

        let all_embedded = MigrationSource::<diesel::pg::Pg>::migrations(&MIGRATIONS)
            .map_err(|e| format!("Error getting embedded migrations: {}", e))?;

        for m in applied {
            let version_str = m.to_string();
            let mut name_str = version_str.clone();

            for emb in &all_embedded {
                if emb.name().version().to_string() == version_str {
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
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Task execution failed: {}", e)).into_response(),
    }
}"#;
    let content = content.replace(handler_target, handler_replacement);
    fs::write(filename, content).unwrap();
}
