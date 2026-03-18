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
        let rows: Vec<_> = range.rows().skip(1).take(r.unwrap_or(std::i32::MAX) as usize).collect();
        let mut batch = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            let d = row[0].as_datetime().unwrap();
            let t_str = row[1].as_string().unwrap().to_string();
            let p_val = helpers::convert(&row[2]).unwrap();
            let s_val = helpers::convert(&row[3]).unwrap();

            batch.push(infra::OwnedObject {
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

