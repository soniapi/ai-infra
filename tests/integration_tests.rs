use ai_infra::*;
use calamine::{open_workbook, Xlsx};
use diesel::{Connection, PgConnection};
use std::fs::File;
use std::io::BufReader;
use chrono::NaiveDate;
use serial_test::serial;
use std::env;

struct EnvGuard {
    key: String,
    original_value: Option<String>,
}

impl EnvGuard {
    fn new(key: &str) -> Self {
        let original_value = env::var(key).ok();
        Self {
            key: key.to_string(),
            original_value,
        }
    }

    fn set(&self, value: &str) {
        unsafe {
            env::set_var(&self.key, value);
        }
    }

    #[allow(dead_code)]
    fn remove(&self) {
        unsafe {
            env::remove_var(&self.key);
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.original_value {
            Some(val) => unsafe {
                env::set_var(&self.key, val);
            },
            None => unsafe {
                env::remove_var(&self.key);
            },
        }
    }
}

fn get_test_connection() -> PgConnection {
    let mut conn = establish_connection();
    conn.begin_test_transaction().unwrap();
    conn
}

#[test]
#[serial]
fn test_divider_sql_positive() {
    let mut conn = get_test_connection();
    let (partition_name_below, partition_name_above, sql_below, sql_above) = divider_sql(&mut conn, 5.5);
    assert_eq!(partition_name_below, "objects_s_below_5.5");
    assert_eq!(partition_name_above, "objects_s_above_5.5");
    assert_eq!(sql_below, "CREATE TABLE \"objects_s_below_5.5\" PARTITION OF objects_s FOR VALUES FROM (MINVALUE) TO ('5.5')");
    assert_eq!(sql_above, "CREATE TABLE \"objects_s_above_5.5\" PARTITION OF objects_s FOR VALUES FROM ('5.5') TO (MAXVALUE)");
}

#[test]
#[serial]
fn test_divider_sql_negative() {
    let mut conn = get_test_connection();
    let (partition_name_below, partition_name_above, sql_below, sql_above) = divider_sql(&mut conn, -2.3);
    assert_eq!(partition_name_below, "objects_s_below_-2.3");
    assert_eq!(partition_name_above, "objects_s_above_-2.3");
    assert_eq!(sql_below, "CREATE TABLE \"objects_s_below_-2.3\" PARTITION OF objects_s FOR VALUES FROM (MINVALUE) TO ('-2.3')");
    assert_eq!(sql_above, "CREATE TABLE \"objects_s_above_-2.3\" PARTITION OF objects_s FOR VALUES FROM ('-2.3') TO (MAXVALUE)");
}

#[test]
#[serial]
    fn test_process_workbook_no_limit() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("tests/test_data.xlsx").unwrap();
        let mut rows_processed = 0;
        let mut skipped_invalid = true;

        process_workbook(&mut excel, "Sheet1", None, |_d, t_val, _p_val, _s_val| {
            rows_processed += 1;
            if t_val == "test3" {
                skipped_invalid = false;
            }
        });

        // We have 4 rows in test_data.xlsx:
        // row 1: valid
        // row 2: valid
        // row 3: invalid (string where datetime expected)
        // row 4: valid
        // But skip(1) skips the header if it was generated as header.
        // skip(1) skips header. So it processes 4 data rows.
        // But row 3 is invalid datetime.
        // So 3 valid rows should be processed.
        assert_eq!(rows_processed, 3);
        assert!(skipped_invalid, "Should have skipped the invalid row");
    }

    #[test]
    #[serial]
    fn test_process_workbook_with_limit() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("tests/test_data.xlsx").unwrap();
        let mut rows_processed = 0;

        process_workbook(&mut excel, "Sheet1", Some(2), |_d, _t_val, _p_val, _s_val| {
            rows_processed += 1;
        });

        // With limit 2, it should take first 2 rows. Both are valid.
        assert_eq!(rows_processed, 2);
    }

    #[test]
    #[serial]
    fn test_process_workbook_invalid_tab() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("tests/test_data.xlsx").unwrap();
        let mut rows_processed = 0;

        process_workbook(&mut excel, "NonExistentTab", None, |_d, _t_val, _p_val, _s_val| {
            rows_processed += 1;
        });

        assert_eq!(rows_processed, 0);
    }

    #[test]
    #[serial]
    fn test_create_object_none_partition() {
        let mut conn = get_test_connection();
        let d = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(12, 0, 0).unwrap();
        let t = "test_t".to_string();
        let p = 1.0;
        let s = 2.0;
        let c = 3.0;

        let result = create_object(&mut conn, None, &d, &t, &p, &s, &c);
        assert!(result.is_ok());

        if let Ok(ObjectType::None(obj)) = result {
            assert_eq!(obj.d, d);
            assert_eq!(obj.t, t);
            assert_eq!(obj.p, p);
            assert_eq!(obj.s, s);
            assert_eq!(obj.c, c);
        } else {
            panic!("Expected ObjectType::None");
        }
    }

    #[test]
    #[serial]
    fn test_create_object_some_s_partition() {
        let mut conn = get_test_connection();
        let d = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(12, 0, 0).unwrap();
        let t = "test_s".to_string();
        let p = 4.0;
        let s = 5.0;
        let c = 6.0;

        let partition_val = "s".to_string();
        let result = create_object(&mut conn, Some(&partition_val), &d, &t, &p, &s, &c);
        assert!(result.is_ok());

        if let Ok(ObjectType::S(obj)) = result {
            assert_eq!(obj.d, d);
            assert_eq!(obj.t, t);
            assert_eq!(obj.p, p);
            assert_eq!(obj.s, s);
            assert_eq!(obj.c, c);
        } else {
            panic!("Expected ObjectType::S");
        }
    }

    #[test]
    #[serial]
    fn test_create_object_invalid_partition() {
        let mut conn = get_test_connection();
        let d = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(12, 0, 0).unwrap();
        let t = "test_invalid".to_string();
        let p = 7.0;
        let s = 8.0;
        let c = 9.0;

        let partition_val = "invalid".to_string();
        let result = create_object(&mut conn, Some(&partition_val), &d, &t, &p, &s, &c);
        assert!(result.is_err());
    }

#[test]
#[serial]
fn test_establish_connection_success() {
    // The test environment should already have a valid DATABASE_URL set in `.env`
    // or system env. We just ensure calling it doesn't panic.
    // establish_connection() will load from .env or env.

    // We can use the guard to ensure it gets restored.
    let _guard = EnvGuard::new("DATABASE_URL");

    // Call should succeed, dotenv().ok() inside establish_connection will set it if not present.
    let _conn = establish_connection();
}

#[test]
#[serial]
fn test_establish_connection_missing_url() {
    let guard = EnvGuard::new("DATABASE_URL");
    // Since we cannot safely change current_dir or rename `.env` in multithreaded tests,
    // and `establish_connection` calls `dotenv().ok()` which will reload `.env` if missing,
    // we can test the error by putting an empty string in DATABASE_URL.
    // This will bypass the "must be set" `expect` but will panic on `establish` with a predictable error,
    // or we can just accept that with `dotenv` it's hard to test the pure "missing" without mocking.
    // Actually, setting it to empty string causes `PgConnection::establish("")` which panics with "Error connecting to ".
    guard.set("");

    let result = std::panic::catch_unwind(|| {
        establish_connection();
    });

    assert!(result.is_err(), "establish_connection should panic when DATABASE_URL is missing or empty");

    if let Err(err) = result {
        let msg = err.downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| err.downcast_ref::<&str>().copied());
        if let Some(s) = msg {
            assert!(s.contains("Error connecting to"), "Panic message was: {}", s);
        }
    }
}

#[test]
#[serial]
fn test_establish_connection_invalid_url() {
    let guard = EnvGuard::new("DATABASE_URL");
    guard.set("postgres://invalid:password@localhost/invalid_db");

    // Call should panic because DATABASE_URL is invalid
    let result = std::panic::catch_unwind(|| {
        establish_connection();
    });

    assert!(result.is_err(), "establish_connection should panic when DATABASE_URL is invalid");

    // Check panic message if possible
    if let Err(err) = result {
        let msg = err.downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| err.downcast_ref::<&str>().copied());
        if let Some(s) = msg {
            assert!(s.contains("Error connecting to postgres://invalid:password@localhost/invalid_db"), "Panic message was: {}", s);
        }
    }
}

struct ProcessGuard(std::process::Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[serial]
async fn test_grpc_hypothesis_context() {
    use std::process::Command;
    use std::time::Duration;
    use ai_infra::schema_service::context_service_client::ContextServiceClient;
    use ai_infra::schema_service::HypothesisContextRequest;

    // Spawn the gRPC server in the background using the compiled binary
    let server_process = Command::new("cargo")
        .args(["run", "--bin", "grpc_schema_service"])
        .spawn()
        .expect("Failed to start grpc_schema_service");

    let _guard = ProcessGuard(server_process);

    let result = async {
        // Wait for the server to start by retrying the connection
        let mut retries = 0;
        let mut client = loop {
            match ContextServiceClient::connect("http://[::1]:50051").await {
                Ok(c) => break c,
                Err(e) => {
                    if retries > 20 {
                        return Err(e.into());
                    }
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };

        let request = tonic::Request::new(HypothesisContextRequest {
            target_table: "objects".to_string(),
            since_timestamp: "2023-01-01".to_string(),
        });

        let response = client.get_hypothesis_context(request).await?;
        let inner = response.into_inner();

        let schema_names: Vec<String> = inner.schema.into_iter().map(|c| c.column_name).collect();
        assert!(schema_names.contains(&"id".to_string()));
        assert!(schema_names.contains(&"d".to_string()));
        assert!(inner.stats.is_empty());

        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;

    // Assert that the test passed
    result.expect("gRPC test failed");
}
