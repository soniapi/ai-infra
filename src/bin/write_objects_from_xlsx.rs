use calamine::{Xlsx, open_workbook, Reader};
use infra::{establish_connection, create_object};

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

    let limit = r.unwrap_or(i32::MAX);

    if let Some(Ok(range)) = excel.worksheet_range(&t) {
        for row in range.rows().skip(1).take(limit as usize) {
            if let (Some(d), Some(t_str), Some(p_val), Some(s_val)) = (
                row[0].as_datetime(),
                row[1].as_string(),
                helpers::convert(&row[2]),
                helpers::convert(&row[3]),
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

