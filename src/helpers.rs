use calamine::DataType;
use std::any::type_name;
use std::io::{self, Write, BufRead};
use std::error::Error;
use std::fmt;

pub fn convert(data: &DataType) -> Option<f32> {
    match data {
        DataType::Float(f) => Some(*f as f32),
        _ => None,
    }
}

pub fn print_type<T>(_: &T) {
    println!("Type is: {}", type_name::<T>());
}

pub fn read_input_option<R: BufRead>(reader: &mut R) -> Option<String> {
    let mut o = String::new();

    match reader.read_line(&mut o) {
        Ok(0) => None,
        Ok(_) =>  {
           let trimmed_o = o.trim_end().to_owned(); 
           if trimmed_o.is_empty() {
                None
            }
            else {
                Some(trimmed_o)
            }
        }
        Err(_) => None, 
    }
}

pub fn inputs_option() -> Option<String> {
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    read_input_option(&mut handle)
}

pub fn inputs_from<R: BufRead, W: Write>(reader: &mut R, writer: &mut W) -> (String, String, Option<String>, Option<i32>) {
    writer.flush().expect("Failed to flush stdout");
    writeln!(writer, "Enter your file path:").unwrap();
    let mut f = String::new();
    reader.read_line(&mut f).expect("Read line failed");
    let trimmed_f = f.trim().to_string();
    writeln!(writer, "Your input:{:?}", trimmed_f).unwrap();

    writer.flush().expect("Failed to flush stdout");
    writeln!(writer, "Enter your file tab:").unwrap();
    let mut t = String::new();
    reader.read_line(&mut t).expect("Read line failed");
    let trimmed_t = t.trim().to_string();
    writeln!(writer, "Your input:{:?}", trimmed_t).unwrap();

    writer.flush().expect("Failed to flush stdout");
    writeln!(writer, "Enter your partition type (no partition press enter):").unwrap();
    let trimmed_p: Option<String> = read_input_option(reader);
    writeln!(writer, "Your input {:?}", trimmed_p).unwrap();

    writer.flush().expect("Failed to flush stdout");
    writeln!(writer, "Enter how many rows to deserialize (all rows press enter):").unwrap();
    let trimmed_r: Option<i32> = read_input_option(reader).and_then(|trimmed_r: String| trimmed_r.parse::<i32>().ok());
    writeln!(writer, "Your input {:?}", trimmed_r).unwrap();

    (trimmed_f, trimmed_t, trimmed_p, trimmed_r)
}

pub fn inputs() -> (String, String, Option<String>, Option<i32>) {
    let mut stdout = io::stdout();
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    inputs_from(&mut handle, &mut stdout)
}

pub fn errors() -> Result<(), Box<dyn Error>> {
    let message = "Error";
    #[derive(Debug)]
    struct BaseError(String); 
    impl fmt::Display for BaseError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{}", self.0)
        }
    }
    impl Error for BaseError {}
    Err(Box::new(BaseError(message.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use calamine::DataType;

    #[test]
    fn test_convert_float() {
        let data = DataType::Float(3.14);
        assert_eq!(convert(&data), Some(3.14));
    }

    #[test]
    fn test_convert_int() {
        let data = DataType::Int(42);
        assert_eq!(convert(&data), None);
    }

    #[test]
    fn test_convert_string() {
        let data = DataType::String("hello".to_string());
        assert_eq!(convert(&data), None);
    }

    #[test]
    fn test_convert_bool() {
        let data = DataType::Bool(true);
        assert_eq!(convert(&data), None);
    }

    #[test]
    fn test_convert_empty() {
        let data = DataType::Empty;
        assert_eq!(convert(&data), None);
    }

    #[test]
    fn test_read_input_option_happy_path() {
        let input = b"valid input\n";
        let mut reader = &input[..];
        assert_eq!(read_input_option(&mut reader), Some("valid input".to_string()));
    }

    #[test]
    fn test_read_input_option_trim_whitespace() {
        let input = b"  test   \n";
        let mut reader = &input[..];
        assert_eq!(read_input_option(&mut reader), Some("  test".to_string()));
    }

    #[test]
    fn test_read_input_option_empty_string() {
        let input = b"\n";
        let mut reader = &input[..];
        assert_eq!(read_input_option(&mut reader), None);
    }

    #[test]
    fn test_read_input_option_whitespace_only() {
        let input = b"   \n";
        let mut reader = &input[..];
        assert_eq!(read_input_option(&mut reader), None);
    }

    #[test]
    fn test_read_input_option_eof() {
        let input = b"";
        let mut reader = &input[..];
        assert_eq!(read_input_option(&mut reader), None);
    }

    #[test]
    fn test_read_input_option_no_newline() {
        let input = b"no newline";
        let mut reader = &input[..];
        assert_eq!(read_input_option(&mut reader), Some("no newline".to_string()));
    }
}

#[cfg(test)]
mod io_tests {
    use super::*;

    #[test]
    fn test_inputs_option_from_empty() {
        let mut reader = &b""[..];
        assert_eq!(read_input_option(&mut reader), None);
    }

    #[test]
    fn test_inputs_option_from_newline_only() {
        let mut reader = &b"\n"[..];
        assert_eq!(read_input_option(&mut reader), None);
    }

    #[test]
    fn test_inputs_option_from_spaces() {
        let mut reader = &b"   \n"[..];
        // trim_end trims spaces too
        assert_eq!(read_input_option(&mut reader), None);
    }

    #[test]
    fn test_inputs_option_from_valid() {
        let mut reader = &b"hello\n"[..];
        assert_eq!(read_input_option(&mut reader), Some("hello".to_string()));
    }

    #[test]
    fn test_inputs_option_from_valid_with_spaces() {
        let mut reader = &b"  hello world  \n"[..];
        assert_eq!(read_input_option(&mut reader), Some("  hello world".to_string()));
    }

    #[test]
    fn test_inputs_from_all_provided() {
        let mut reader = &b"my_file.xlsx\nSheet1\npart_1\n100\n"[..];
        let mut writer = Vec::new();
        let (f, t, p, r) = inputs_from(&mut reader, &mut writer);

        assert_eq!(f, "my_file.xlsx");
        assert_eq!(t, "Sheet1");
        assert_eq!(p, Some("part_1".to_string()));
        assert_eq!(r, Some(100));

        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("Enter your file path:"));
        assert!(output.contains("Your input:\"my_file.xlsx\""));
        assert!(output.contains("Enter your file tab:"));
        assert!(output.contains("Your input:\"Sheet1\""));
        assert!(output.contains("Enter your partition type (no partition press enter):"));
        assert!(output.contains("Your input Some(\"part_1\")"));
        assert!(output.contains("Enter how many rows to deserialize (all rows press enter):"));
        assert!(output.contains("Your input Some(100)"));
    }

    #[test]
    fn test_inputs_from_optional_missing() {
        let mut reader = &b"my_file.xlsx\nSheet1\n\n\n"[..];
        let mut writer = Vec::new();
        let (f, t, p, r) = inputs_from(&mut reader, &mut writer);

        assert_eq!(f, "my_file.xlsx");
        assert_eq!(t, "Sheet1");
        assert_eq!(p, None);
        assert_eq!(r, None);

        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("Your input None"));
    }

    #[test]
    fn test_inputs_from_invalid_integer() {
        let mut reader = &b"my_file.xlsx\nSheet1\npart_1\nnot_a_number\n"[..];
        let mut writer = Vec::new();
        let (f, t, p, r) = inputs_from(&mut reader, &mut writer);

        assert_eq!(f, "my_file.xlsx");
        assert_eq!(t, "Sheet1");
        assert_eq!(p, Some("part_1".to_string()));
        assert_eq!(r, None);

        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("Your input None"));
    }
}
