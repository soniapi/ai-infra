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
            return;
        }
    };

    if let Some(Ok(range)) = excel.worksheet_range(&t) {
        for row in range.rows().skip(1).take(r.unwrap() as usize) {
            println!("Check you PostgreSQL table for below object insertion");
            println!("row[0]={:?}, row[1]={:?}, row[2]={:?}, row[3]={:?}", row[0].as_datetime(), row[1].as_string(), &helpers::convert(&row[2]).as_ref().unwrap(), &helpers::convert(&row[3]).as_ref().unwrap());
            let _ = create_object(connection,  p.as_ref(), row[0].as_datetime().as_ref().unwrap(), row[1].as_string().as_ref().unwrap(), helpers::convert(&row[2]).as_ref().unwrap(), helpers::convert(&row[3]).as_ref().unwrap(), &0.0);
        }
    }
    else {
        println!("Can't find the file.");
    }
}

