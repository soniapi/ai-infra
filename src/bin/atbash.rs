use std::env;
use std::io::{self, Read};

fn atbash_cipher(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() {
                (b'z' - (c as u8 - b'a')) as char
            } else if c.is_ascii_uppercase() {
                (b'Z' - (c as u8 - b'A')) as char
            } else {
                c
            }
        })
        .collect()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut input = String::new();

    if args.len() > 1 {
        input = args[1..].join(" ");
    } else {
        io::stdin().read_to_string(&mut input).expect("Failed to read from stdin");
    }

    let result = atbash_cipher(&input);
    print!("{}", result);
}
