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

pub fn inputs_option() -> Option<String> {
    let stdin = io::stdin();
    let mut o = String::new();

    match stdin.lock().read_line(&mut o) {
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

pub fn inputs() -> (String, String, Option<String>, Option<i32>) {
    let mut stdout = io::stdout();
    let stdin = io::stdin();

    stdout.flush().expect("Failed to flush stdout");
    println!("Enter your file path:");
    let mut f = String::new();
    stdin.read_line(&mut f).expect("Read line failed");
    let trimmed_f = f.trim().to_string();
    println!("Your input:{:?}", trimmed_f);

    stdout.flush().expect("Failed to flush stdout");
    println!("Enter your file tab:");
    let mut t = String::new();
    stdin.read_line(&mut t).expect("Read line failed");
    let trimmed_t = t.trim().to_string();
    println!("Your input:{:?}", trimmed_t);

    stdout.flush().expect("Failed to flush stdout");
    println!("Enter your partition type (no partition press enter):");
    let trimmed_p: Option<String> = inputs_option();
    println!("Your input {:?}", trimmed_p);

    stdout.flush().expect("Failed to flush stdout");
    println!("Enter how many rows to deserialize (all rows press enter):");
    let trimmed_r: Option<i32> = inputs_option().and_then(|trimmed_r| trimmed_r.parse::<i32>().ok());
    println!("Your input {:?}", trimmed_r);

    (trimmed_f, trimmed_t, trimmed_p, trimmed_r)
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
}
