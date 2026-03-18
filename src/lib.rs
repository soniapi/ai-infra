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
    let mut excel: Xlsx<_> = open_workbook(f).unwrap();

    match r {
        Some(limit) => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                for row in range.rows().skip(1).take(limit as usize) {
                    println!("Check you PostgreSQL table for below object insertion");
                    println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", row[0].as_datetime(), row[1].as_string(), &helpers::convert(&row[2]).as_ref().unwrap(), &helpers::convert(&row[3]).as_ref().unwrap());
                    let _ = create_object(connection,  p.as_ref(), row[0].as_datetime().as_ref().unwrap(), row[1].as_string().as_ref().unwrap(), convert(&row[2]).as_ref().unwrap(), convert(&row[3]).as_ref().unwrap(), &0.0);
                 }
            }
            else {
                println!("Can't find the file.");
            }
        }
        None => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                for row in range.rows().skip(1) {
                    println!("Check you PostgreSQL table for below object insertion");
                    println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", row[0].as_datetime(), row[1].as_string(), &helpers::convert(&row[2]).as_ref().unwrap(), &helpers::convert(&row[3]).as_ref().unwrap());
                    let _ = create_object(connection,  p.as_ref(), row[0].as_datetime().as_ref().unwrap(), row[1].as_string().as_ref().unwrap(), convert(&row[2]).as_ref().unwrap(), convert(&row[3]).as_ref().unwrap(), &0.0);
                 }
            }
            else {
                println!("Can't find the file.");
            }
        } 
    }
}