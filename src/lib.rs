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

pub struct OwnedObject {
    pub d: NaiveDateTime,
    pub t: String,
    pub p: f32,
    pub s: f32,
    pub c: f32,
}

pub fn create_objects(connection: &mut PgConnection, partition: Option<&String>, objects: &[OwnedObject]) -> Result<usize, Box<dyn Error>> {
    match partition {
        None => {
            println!("No partition, inserting {} objects", objects.len());
            use crate::schema::objects;
            let mut inserted_count = 0;

            let new_objects: Vec<NewObject> = objects.iter().map(|o| NewObject {
                d: &o.d,
                t: &o.t,
                p: &o.p,
                s: &o.s,
                c: &o.c,
            }).collect();

            // PostgreSQL has a limit of 65535 parameters per query.
            // We have 5 parameters per object, so we can insert at most 13107 objects at a time.
            for chunk in new_objects.chunks(10000) {
                match diesel::insert_into(objects::table)
                    .values(chunk)
                    .execute(connection)
                {
                    Ok(count) => inserted_count += count,
                    Err(e) => println!("Error saving new objects chunk: {}", e),
                }
            }
            Ok(inserted_count)
        },
        Some(value) if value == "s" => {
            println!("Partition: {:?}, inserting {} objects", value, objects.len());
            use crate::schema::objects_s;
            let mut inserted_count = 0;

            let new_objects: Vec<NewObjectS> = objects.iter().map(|o| NewObjectS {
                d: &o.d,
                t: &o.t,
                p: &o.p,
                s: &o.s,
                c: &o.c,
            }).collect();

            for chunk in new_objects.chunks(10000) {
                match diesel::insert_into(objects_s::table)
                    .values(chunk)
                    .execute(connection)
                {
                    Ok(count) => inserted_count += count,
                    Err(e) => println!("Error saving new objects_s chunk in partitioned table: {}", e),
                }
            }
            Ok(inserted_count)
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
                let rows: Vec<_> = range.rows().skip(1).take(limit as usize).collect();
                let mut batch = Vec::with_capacity(rows.len());

                for row in rows.into_iter() {
                    let d = row[0].as_datetime().unwrap();
                    let t_str = row[1].as_string().unwrap().to_string();
                    let p_val = convert(&row[2]).unwrap();
                    let s_val = convert(&row[3]).unwrap();

                    batch.push(OwnedObject {
                        d,
                        t: t_str,
                        p: p_val,
                        s: s_val,
                        c: 0.0,
                    });
                }

                if !batch.is_empty() {
                    let _ = create_objects(connection, p.as_ref(), &batch);
                }
            }
            else {
                println!("Can't find the file.");
            }
        }
        None => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                let rows: Vec<_> = range.rows().skip(1).collect();
                let mut batch = Vec::with_capacity(rows.len());

                for row in rows.into_iter() {
                    let d = row[0].as_datetime().unwrap();
                    let t_str = row[1].as_string().unwrap().to_string();
                    let p_val = convert(&row[2]).unwrap();
                    let s_val = convert(&row[3]).unwrap();

                    batch.push(OwnedObject {
                        d,
                        t: t_str,
                        p: p_val,
                        s: s_val,
                        c: 0.0,
                    });
                }

                if !batch.is_empty() {
                    let _ = create_objects(connection, p.as_ref(), &batch);
                }
            }
            else {
                println!("Can't find the file.");
            }
        } 
    }
}