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
            let new_object = NewObject { d, t, p, s, c };
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
            let new_object_s = NewObjectS { d, t, p, s, c };
            Ok(ObjectType::S(diesel::insert_into(objects_s::table)
                .values(&new_object_s)
                .returning(ObjectS::as_returning())
                .get_result(connection)
                .expect("Error saving new object_s in partioned table")))
        },
        _ => Err("Error".into()),
    }
}

pub fn fill_partitions() {
    let connection = &mut establish_connection();
    let (f, t, p, r) = helpers::inputs();
    let mut excel: Xlsx<_> = match open_workbook(&f) {
        Ok(workbook) => workbook,
        Err(e) => {
            println!("Error opening workbook {}: {}", f, e);
            return;
        }
    };

    match r {
        Some(limit) => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                for row in range.rows().skip(1).take(limit as usize) {
                    if let (Some(d), Some(t_str), Some(p_val), Some(s_val)) = (
                        row[0].as_datetime(),
                        row[1].as_string(),
                        convert(&row[2]),
                        convert(&row[3]),
                    ) {
                        println!("Check you PostgreSQL table for below object insertion");
                        println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", d, t_str, p_val, s_val);
                        let _ = create_object(connection, p.as_ref(), &d, &t_str, &p_val, &s_val, &0.0);
                    } else {
                        println!("Skipping row due to invalid data: {:?}", row);
                    }
                 }
            }
            else {
                println!("Can't find the file.");
            }
        }
        None => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                for row in range.rows().skip(1) {
                    if let (Some(d), Some(t_str), Some(p_val), Some(s_val)) = (
                        row[0].as_datetime(),
                        row[1].as_string(),
                        convert(&row[2]),
                        convert(&row[3]),
                    ) {
                        println!("Check you PostgreSQL table for below object insertion");
                        println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", d, t_str, p_val, s_val);
                        let _ = create_object(connection, p.as_ref(), &d, &t_str, &p_val, &s_val, &0.0);
                    } else {
                        println!("Skipping row due to invalid data: {:?}", row);
                    }
                 }
            }
            else {
                println!("Can't find the file.");
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

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
