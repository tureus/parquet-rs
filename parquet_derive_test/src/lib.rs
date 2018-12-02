extern crate parquet;
#[macro_use]
extern crate parquet_derive;

//use parquet::file::writer::SerializedRowGroupWriter;
use parquet::file::writer::RecordWriter;

#[derive(ParquetRecordWriter)]
struct ACompleteRecord<'a> {
    pub a_bool: bool,
    pub a_str: &'a str,
    pub a_string: String,
    pub a_borrowed_string: &'a String,
    pub maybe_a_str: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::rc::Rc;
    use parquet::schema::{
        parser::parse_message_type
    };
    use parquet::file::{
            properties::WriterProperties,
            writer::{ FileWriter, SerializedFileWriter }
    };
    use std::{fs, env, io::Write};

    #[test]
    fn hello() {
        let file = get_temp_file("test_parquet_derive_hello", &[]);
        let schema_str = "message schema {
            REQUIRED boolean a_bool;
            REQUIRED BINARY a_str (UTF8);
            REQUIRED BINARY a_string (UTF8);
            REQUIRED BINARY a_borrowed_string (UTF8);
            OPTIONAL BINARY a_maybe_str (UTF8);
        }";
        let schema = Rc::new(parse_message_type(schema_str).unwrap());

        let props = Rc::new(WriterProperties::builder().build());
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

        let a_str = "hello mother".to_owned();
        let a_borrowed_string = "cool news".to_owned();
        let drs : Vec<ACompleteRecord> = vec![
            ACompleteRecord {
                a_bool: true,
                a_str: &a_str[..],
                a_string: "hello father".into(),
                a_borrowed_string: &a_borrowed_string,
                maybe_a_str: Some(&a_str[..]),
            }
        ];
        let chunks = &drs[..];

        let mut row_group = writer.next_row_group().unwrap();
        chunks.write_to_row_group(&mut row_group);
        writer.close_row_group(row_group).unwrap();
        writer.close().unwrap();
    }

    /// Returns file handle for a temp file in 'target' directory with a provided content
    pub fn get_temp_file(file_name: &str, content: &[u8]) -> fs::File {
        // build tmp path to a file in "target/debug/testdata"
        let mut path_buf = env::current_dir().unwrap();
        path_buf.push("target");
        path_buf.push("debug");
        path_buf.push("testdata");
        fs::create_dir_all(&path_buf).unwrap();
        path_buf.push(file_name);

        // write file content
        let mut tmp_file = fs::File::create(path_buf.as_path()).unwrap();
        tmp_file.write_all(content).unwrap();
        tmp_file.sync_all().unwrap();

        // return file handle for both read and write
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path_buf.as_path());
        assert!(file.is_ok());
        file.unwrap()
    }

}