use diesel::prelude::*;
use dotenvy::dotenv;
use std::env;
use chrono::NaiveDateTime;
use std::error::Error;
use calamine::{Xlsx, open_workbook, Reader};
use crate::helpers::convert;

pub mod models;
pub mod schema;
pub mod helpers;

pub enum ObjectType {
    None(Object),
    S(ObjectS),
}

pub fn establish_connection() -> PgConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    PgConnection::establish(&database_url).unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

use self::models::{NewObject, Object, NewObjectS, ObjectS};

pub fn create_object(connection: &mut PgConnection, partition: Option<&String>, d: &NaiveDateTime, t: &String, p: &f32, s: &f32, c: &f32) -> Result<ObjectType, Box<dyn Error>> {
    match partition {
        None => {
            println!("No partition");
            use crate::schema::objects;
            let new_object = NewObject { d: *d, t: t.clone(), p: *p, s: *s, c: *c };
            Ok(ObjectType::None(
                diesel::insert_into(objects::table)
                .values(&new_object)
                .returning(Object::as_returning())
                .get_result(connection)
                .expect("Error saving new object")))
        },
        Some(value) if value == "s" => {
            println!("Partition: {:?}", value);
            use crate::schema::objects_s;
            let new_object_s = NewObjectS { d: *d, t: t.clone(), p: *p, s: *s, c: *c };
            Ok(ObjectType::S(diesel::insert_into(objects_s::table)
                .values(&new_object_s)
                .returning(ObjectS::as_returning())
                .get_result(connection)
                .expect("Error saving new object_s in partioned table")))
        },
        _ => Err("Error".into()),
    }
}

pub fn create_objects(connection: &mut PgConnection, objects: &[NewObject]) -> Result<usize, Box<dyn Error>> {
    use crate::schema::objects;
    let count = diesel::insert_into(objects::table)
        .values(objects)
        .execute(connection)?;
    Ok(count)
}

pub fn create_objects_s(connection: &mut PgConnection, objects: &[NewObjectS]) -> Result<usize, Box<dyn Error>> {
    use crate::schema::objects_s;
    let count = diesel::insert_into(objects_s::table)
        .values(objects)
        .execute(connection)?;
    Ok(count)
}

use std::io::{Read, Seek};

pub fn process_workbook<RS: Read + Seek, R: Reader<RS>, F>(excel: &mut R, t: &str, r: Option<i32>, mut handler: F)
where
    F: FnMut(&NaiveDateTime, &String, f32, f32),
{
    if let Some(Ok(range)) = excel.worksheet_range(t) {
        let rows = range.rows().skip(1);
        let row_iter: Box<dyn Iterator<Item = &[calamine::DataType]> + '_> = if let Some(limit) = r {
            Box::new(rows.take(limit as usize))
        } else {
            Box::new(rows)
        };
        for row in row_iter {
            if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                row[0].as_datetime(),
                row[1].as_string(),
                convert(&row[2]),
                convert(&row[3]),
            ) {
                handler(&d, &t_val, p_val, s_val);
            }
        }
    } else {
        println!("Can't find the file or tab.");
    }
}

pub fn fill_partitions() {
    let connection = &mut establish_connection();
    let (f, t, p, r) = helpers::inputs();
    let mut excel: Xlsx<_> = open_workbook(f).unwrap();

    let is_partition_s = p.as_deref() == Some("s");
    let mut objects = Vec::new();
    let mut objects_s = Vec::new();

    process_workbook(&mut excel, &t, r, |d, t_val, p_val, s_val| {
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
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use serial_test::serial;

    // Helper to safely run tests that modify env vars
    fn run_with_env<F>(key: &str, value: Option<&str>, test: F)
    where
        F: FnOnce() + std::panic::UnwindSafe,
    {
        let original = env::var_os(key);
        unsafe {
            if let Some(v) = value {
                env::set_var(key, v);
            } else {
                env::remove_var(key);
            }
        }

        struct EnvGuard {
            key: String,
            original: Option<std::ffi::OsString>,
        }
    use std::fs::File;
    use std::io::BufReader;
    use chrono::NaiveDate;

    #[test]
    fn test_process_workbook_no_limit() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("test_data.xlsx").unwrap();
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
    fn test_process_workbook_with_limit() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("test_data.xlsx").unwrap();
        let mut rows_processed = 0;

        process_workbook(&mut excel, "Sheet1", Some(2), |_d, _t_val, _p_val, _s_val| {
            rows_processed += 1;
        });

        // With limit 2, it should take first 2 rows. Both are valid.
        assert_eq!(rows_processed, 2);
    }

    #[test]
    fn test_process_workbook_invalid_tab() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("test_data.xlsx").unwrap();
        let mut rows_processed = 0;

        process_workbook(&mut excel, "NonExistentTab", None, |_d, _t_val, _p_val, _s_val| {
            rows_processed += 1;
        });

        assert_eq!(rows_processed, 0);
    }

    fn get_test_connection() -> PgConnection {
        let mut conn = establish_connection();
        conn.begin_test_transaction().unwrap();
        conn
    }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                unsafe {
                    if let Some(orig) = &self.original {
                        env::set_var(&self.key, orig);
                    } else {
                        env::remove_var(&self.key);
                    }
                }
            }
        }

        let _guard = EnvGuard {
            key: key.to_string(),
            original,
        };

        test();
    }

    #[test]
    #[serial]
    #[should_panic(expected = "DATABASE_URL must be set")]
    fn test_establish_connection_missing_url() {
        run_with_env("DATABASE_URL", None, || {
            establish_connection();
        });
    }

    #[test]
    #[serial]
    #[should_panic(expected = "Error connecting to invalid_url")]
    fn test_establish_connection_invalid_url() {
        run_with_env("DATABASE_URL", Some("invalid_url"), || {
            establish_connection();
        });
    }
}
