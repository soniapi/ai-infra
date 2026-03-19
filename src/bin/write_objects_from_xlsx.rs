use calamine::{Xlsx, open_workbook, Reader};
use infra::{establish_connection, create_objects};

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
        let limit = r.unwrap_or(std::i32::MAX);

        for row in range.rows().skip(1).take(limit as usize) {
            if let (Some(d), Some(t_str), Some(p_val), Some(s_val)) = (
                row[0].as_datetime(),
                row[1].as_string(),
                helpers::convert(&row[2]),
                helpers::convert(&row[3])
            ) {
                batch.push((d, t_str, p_val, s_val, 0.0));

                if batch.len() >= 1000 {
                    let _ = create_objects(connection, p.as_ref(), &batch);
                    batch.clear();
                }
            } else {
                println!("Skipping row due to invalid data formatting");
            }
        }

        if !batch.is_empty() {
            let _ = create_objects(connection, p.as_ref(), &batch);
        }
        println!("Batch insert completed.");
    }
    else {
        println!("Can't find the file.");
    }
    else {
        println!("Can't find the file.");
    }
}

