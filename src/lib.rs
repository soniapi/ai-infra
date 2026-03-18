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

pub fn fill_partitions() {
    let connection = &mut establish_connection();
    let (f, t, p, r) = helpers::inputs();
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
    }
}