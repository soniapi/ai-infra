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

    if let Some(Ok(range)) = excel.worksheet_range(&t) {
        let mut batch = Vec::new();
        let mut batch_s = Vec::new();
        let is_partition_s = p.as_deref() == Some("s");
        let limit = r.unwrap_or(std::i32::MAX);

        for row in range.rows().skip(1).take(limit as usize) {
            if let (Some(d), Some(t_str), Some(p_val), Some(s_val)) = (
                row[0].as_datetime(),
                row[1].as_string(),
                helpers::convert(&row[2]),
                helpers::convert(&row[3])
            ) {
                if is_partition_s {
                    batch_s.push(NewObjectS {
                        d,
                        t: t_str.to_string(),
                        p: p_val,
                        s: s_val,
                        c: 0.0,
                    });
                    if batch_s.len() >= 1000 {
                        let _ = create_objects_s(connection, &batch_s);
                        batch_s.clear();
                    }
                } else {
                    batch.push(NewObject {
                        d,
                        t: t_str.to_string(),
                        p: p_val,
                        s: s_val,
                        c: 0.0,
                    });
                    if batch.len() >= 1000 {
                        let _ = create_objects(connection, &batch);
                        batch.clear();
                    }
                }
            } else {
                println!("Skipping row due to invalid data formatting");
            }
        }

        if !batch_s.is_empty() {
            let _ = create_objects_s(connection, &batch_s);
        }
        if !batch.is_empty() {
            let _ = create_objects(connection, &batch);
        }
        println!("Batch insert completed.");
    }
    else {
        println!("Can't find the file.");
    }
}

