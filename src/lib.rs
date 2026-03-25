use diesel::prelude::*;
use diesel::sql_query;
use diesel::RunQueryDsl;
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
    let mut excel: Xlsx<_> = match open_workbook(&f) {
        Ok(workbook) => workbook,
        Err(e) => {
            println!("Error opening workbook {}: {}", f, e);
            return;
        }
    };

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
    use std::fs::File;
    use std::io::BufReader;
    use chrono::NaiveDate;

    fn get_test_connection() -> PgConnection {
        let mut conn = establish_connection();
        conn.begin_test_transaction().unwrap();
        conn
    }

    #[test]
    fn test_divider_sql_positive() {
        let mut conn = get_test_connection();
        let (partition_name_below, partition_name_above, sql_below, sql_above) = divider_sql(&mut conn, 5.5);
        assert_eq!(partition_name_below, "objects_s_below_5.5");
        assert_eq!(partition_name_above, "objects_s_above_5.5");
        assert_eq!(sql_below, "CREATE TABLE \"objects_s_below_5.5\" PARTITION OF objects_s FOR VALUES FROM (MINVALUE) TO ('5.5')");
        assert_eq!(sql_above, "CREATE TABLE \"objects_s_above_5.5\" PARTITION OF objects_s FOR VALUES FROM ('5.5') TO (MAXVALUE)");
    }

    #[test]
    fn test_divider_sql_negative() {
        let mut conn = get_test_connection();
        let (partition_name_below, partition_name_above, sql_below, sql_above) = divider_sql(&mut conn, -2.3);
        assert_eq!(partition_name_below, "objects_s_below_-2.3");
        assert_eq!(partition_name_above, "objects_s_above_-2.3");
        assert_eq!(sql_below, "CREATE TABLE \"objects_s_below_-2.3\" PARTITION OF objects_s FOR VALUES FROM (MINVALUE) TO ('-2.3')");
        assert_eq!(sql_above, "CREATE TABLE \"objects_s_above_-2.3\" PARTITION OF objects_s FOR VALUES FROM ('-2.3') TO (MAXVALUE)");
    }

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

#[derive(diesel::query_builder::QueryId, diesel::QueryableByName)]
struct DdlResult {
    #[diesel(sql_type = diesel::sql_types::Text)]
    ddl: String,
}

pub(crate) fn divider_sql(conn: &mut PgConnection, divider_value: f32) -> (String, String, String, String) {
    let partitioned_table = "objects_s";
    let below = "_below_";
    let above = "_above_";

    let partition_name_below = format!(
        "{}{}{}",
        partitioned_table,
        below,
        divider_value.to_string()
    );
    let partition_name_above = format!(
        "{}{}{}",
        partitioned_table,
        above,
        divider_value.to_string()
    );

    let sql_below = sql_query("SELECT format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (MINVALUE) TO (%L)', $1, $2, $3) as ddl")
        .bind::<diesel::sql_types::Text, _>(&partition_name_below)
        .bind::<diesel::sql_types::Text, _>(partitioned_table)
        .bind::<diesel::sql_types::Text, _>(&divider_value.to_string())
        .load::<DdlResult>(conn)
        .expect("Failed to construct sql_below")
        .pop()
        .unwrap()
        .ddl;

    let sql_above = sql_query("SELECT format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (MAXVALUE)', $1, $2, $3) as ddl")
        .bind::<diesel::sql_types::Text, _>(&partition_name_above)
        .bind::<diesel::sql_types::Text, _>(partitioned_table)
        .bind::<diesel::sql_types::Text, _>(&divider_value.to_string())
        .load::<DdlResult>(conn)
        .expect("Failed to construct sql_above")
        .pop()
        .unwrap()
        .ddl;

    (
        partition_name_below,
        partition_name_above,
        sql_below,
        sql_above,
    )
}

fn check_table_exists(conn: &mut PgConnection, table_name: &str) -> bool {
    #[derive(diesel::query_builder::QueryId, diesel::QueryableByName)]
    struct ExistsResult {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        exists: bool,
    }

    let query = sql_query(
        "SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename = $1
        ) as exists",
    )
    .bind::<diesel::sql_types::Text, _>(table_name);

    if let Ok(mut results) = query.load::<ExistsResult>(conn) {
        if let Some(res) = results.pop() {
            return res.exists;
        }
    }
    false
}

fn check_table_health(conn: &mut PgConnection, table_name: &str) -> bool {
    let query = sql_query("SELECT format('SELECT 1 FROM %I LIMIT 1', $1) as ddl")
        .bind::<diesel::sql_types::Text, _>(table_name);

    if let Ok(mut results) = query.load::<DdlResult>(conn) {
        if let Some(res) = results.pop() {
            return sql_query(res.ddl).execute(conn).is_ok();
        }
    }
    false
}

pub fn divider(connection: &mut PgConnection, divider_value: f32) {
    let (partition_name_below, partition_name_above, sql_below, sql_above) =
        divider_sql(connection, divider_value);

    println!(
        "Partition names: {:?} and {:?}",
        partition_name_below, partition_name_above
    );

    let exists_below = check_table_exists(connection, &partition_name_below);
    let exists_above = check_table_exists(connection, &partition_name_above);

    if exists_below && exists_above {
        let healthy_below = check_table_health(connection, &partition_name_below);
        let healthy_above = check_table_health(connection, &partition_name_above);

        if healthy_below && healthy_above {
            println!(
                "Partitions {:?} and {:?} already exist and are healthy. Skipping creation.",
                partition_name_below, partition_name_above
            );
            return;
        }
    }

    sql_query(sql_below)
        .execute(connection)
        .expect("Partition can't be created");

    sql_query(sql_above)
        .execute(connection)
        .expect("Partition can't be created");
}
