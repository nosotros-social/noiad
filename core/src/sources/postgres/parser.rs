use anyhow::Result;
use core::event::Event;

use crate::errors::DataflowError;

pub struct CopyParser<'a> {
    lines: std::str::SplitTerminator<'a, char>,
    column_delimiter: u8,
}

impl<'a> Iterator for CopyParser<'a> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.lines.next()?;
        let mut parts = line.split(self.column_delimiter as char);
        let id = parts.next()?.to_string();
        let pubkey = parts.next()?.to_string();
        let created_at = parts.next()?.parse().ok()?;
        let kind = parts.next()?.parse().ok()?;
        let tags_str = parts.next()?;
        let tags = serde_json::from_str::<Vec<Vec<String>>>(tags_str).unwrap_or_default();
        let content = parts.next()?.to_string();

        Some(Event {
            id,
            pubkey,
            created_at,
            kind,
            tags,
            content,
        })
    }
}

impl<'a> CopyParser<'a> {
    pub fn new(data: &'a [u8], column_delimiter: u8) -> Result<Self, DataflowError> {
        let text = std::str::from_utf8(data)
            .map_err(|_| DataflowError::DecodeError("Invalid UTF-8 sequence".into()))?;
        Ok(Self {
            lines: text.split_terminator('\n'),
            column_delimiter,
        })
    }
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
