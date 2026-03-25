use ai_infra::*;
use calamine::{open_workbook, Xlsx};
use diesel::{Connection, PgConnection};
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
        let mut excel: Xlsx<BufReader<File>> = open_workbook("tests/test_data.xlsx").unwrap();
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
        let mut excel: Xlsx<BufReader<File>> = open_workbook("tests/test_data.xlsx").unwrap();
        let mut rows_processed = 0;

        process_workbook(&mut excel, "Sheet1", Some(2), |_d, _t_val, _p_val, _s_val| {
            rows_processed += 1;
        });

        // With limit 2, it should take first 2 rows. Both are valid.
        assert_eq!(rows_processed, 2);
    }

    #[test]
    fn test_process_workbook_invalid_tab() {
        let mut excel: Xlsx<BufReader<File>> = open_workbook("tests/test_data.xlsx").unwrap();
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
