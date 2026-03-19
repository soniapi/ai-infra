use calamine::{Xlsx, open_workbook, Reader};
use infra::{establish_connection, create_objects, create_objects_s};
use infra::models::{NewObject, NewObjectS};

use infra::helpers;

fn main() {
    let connection = &mut establish_connection();

    let (f, t, p, r) = helpers::inputs();

    let mut excel: Xlsx<_> = match open_workbook(&f) {
        Ok(workbook) => workbook,
        Err(e) => {
            println!("Error opening workbook {}: {}", f, e);
            std::process::exit(1);
        }
    };

<<<<<<< feat/optimize-db-insertions-12902756851123970598
    let is_partition_s = p.as_deref() == Some("s");

    if let Some(Ok(range)) = excel.worksheet_range(&t) {
        let mut objects = Vec::new();
        let mut objects_s = Vec::new();

        let rows = if let Some(limit) = r {
            Box::new(range.rows().skip(1).take(limit as usize)) as Box<dyn Iterator<Item = &[calamine::DataType]> + '_>
        } else {
            Box::new(range.rows().skip(1)) as Box<dyn Iterator<Item = &[calamine::DataType]> + '_>
        };

        for row in rows {
            if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                row[0].as_datetime(),
                row[1].as_string(),
                helpers::convert(&row[2]),
                helpers::convert(&row[3])
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
    } else {
        println!("Can't find the file.");
=======
    match r {
        Some(limit) => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                for row in range.rows().skip(1).take(limit as usize) {
                    if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                        row[0].as_datetime(),
                        row[1].as_string(),
                        helpers::convert(&row[2]),
                        helpers::convert(&row[3]),
                    ) {
                        println!("Check you PostgreSQL table for below object insertion");
                        println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", d, t_val, p_val, s_val);
                        let _ = create_object(connection, p.as_ref(), &d, &t_val, &p_val, &s_val, &0.0);
                    } else {
                        println!("Skipping row due to invalid data format");
                    }
                }
            } else {
                println!("Can't find the file.");
            }
        }
        None => {
            if let Some(Ok(range)) = excel.worksheet_range(&t) {
                for row in range.rows().skip(1) {
                    if let (Some(d), Some(t_val), Some(p_val), Some(s_val)) = (
                        row[0].as_datetime(),
                        row[1].as_string(),
                        helpers::convert(&row[2]),
                        helpers::convert(&row[3]),
                    ) {
                        println!("Check you PostgreSQL table for below object insertion");
                        println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", d, t_val, p_val, s_val);
                        let _ = create_object(connection, p.as_ref(), &d, &t_val, &p_val, &s_val, &0.0);
                    } else {
                        println!("Skipping row due to invalid data format");
                    }
                }
            } else {
                println!("Can't find the file.");
            }
        }
>>>>>>> master
    }
}

