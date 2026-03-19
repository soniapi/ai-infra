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

<<<<<<< feat/optimize-db-insertions-12902756851123970598
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
=======
use std::io::{Read, Seek};

pub fn process_workbook<RS: Read + Seek, R: Reader<RS>, F>(excel: &mut R, t: &str, r: Option<i32>, mut handler: F)
where
    F: FnMut(&NaiveDateTime, &String, f32, f32),
{
    if let Some(Ok(range)) = excel.worksheet_range(t) {
        let rows = range.rows().skip(1);
        if let Some(limit) = r {
            for row in rows.take(limit as usize) {
                if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                    row[0].as_datetime(),
                    row[1].as_string(),
                    convert(&row[2]),
                    convert(&row[3]),
                ) {
                    println!("Check your PostgreSQL table for below object insertion");
                    println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", d, t_val, p_val, s_val);
                    handler(&d, &t_val, p_val, s_val);
                } else {
                    eprintln!("Warning: Skipping invalid row: {:?}", row);
                }
            }
        } else {
            for row in rows {
                if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                    row[0].as_datetime(),
                    row[1].as_string(),
                    convert(&row[2]),
                    convert(&row[3]),
                ) {
                    println!("Check your PostgreSQL table for below object insertion");
                    println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", d, t_val, p_val, s_val);
                    handler(&d, &t_val, p_val, s_val);
                } else {
                    eprintln!("Warning: Skipping invalid row: {:?}", row);
                }
            }
        }
    } else {
        println!("Can't find the file or tab.");
    }
>>>>>>> master
}

pub fn fill_partitions() {
    let connection = &mut establish_connection();
    let (f, t, p, r) = helpers::inputs();
<<<<<<< feat/optimize-db-insertions-12902756851123970598
    let mut excel: Xlsx<_> = open_workbook(f).unwrap();

    let is_partition_s = p.as_deref() == Some("s");

    let mut process_rows = |rows: Box<dyn Iterator<Item = &[calamine::DataType]> + '_>| {
        let mut objects = Vec::new();
        let mut objects_s = Vec::new();

        for row in rows {
            if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                row[0].as_datetime(),
                row[1].as_string(),
                convert(&row[2]),
                convert(&row[3])
            ) {
                if is_partition_s {
                    objects_s.push(NewObjectS {
                        d,
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
                        d,
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
            }
        }

        if !objects_s.is_empty() {
            let _ = create_objects_s(connection, &objects_s);
        }
        if !objects.is_empty() {
            let _ = create_objects(connection, &objects);
        }
    };

    if let Some(Ok(range)) = excel.worksheet_range(&t) {
        match r {
            Some(limit) => {
                process_rows(Box::new(range.rows().skip(1).take(limit as usize)));
            }
            None => {
                process_rows(Box::new(range.rows().skip(1)));
            }
        }
    } else {
        println!("Can't find the file.");
=======
    let mut excel: Xlsx<_> = match open_workbook(&f) {
        Ok(workbook) => workbook,
        Err(e) => {
            println!("Error opening workbook {}: {}", f, e);
            return;
        }
    };

    process_workbook(&mut excel, &t, r, |d, t_val, p_val, s_val| {
        let _ = create_object(connection, p.as_ref(), d, t_val, &p_val, &s_val, &0.0);
    });
}
#[cfg(test)]
mod tests {
    use super::*;
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
>>>>>>> master
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

    #[test]
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
}
