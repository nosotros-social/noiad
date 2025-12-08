use crate::event::{EventRow, Tags, decode_hex};

pub struct CopyParser<'a> {
    data: &'a [u8],
    column_delimiter: u8,
}

impl<'a> CopyParser<'a> {
    pub fn new(data: &'a [u8], column_delimiter: u8) -> Self {
        Self {
            data,
            column_delimiter,
        }
    }

    pub fn iter_rows(&self) -> impl Iterator<Item = EventRow> + '_ {
        let delimiter = self.column_delimiter;
        let mut rows = self
            .data
            .split(|&b| b == b'\n')
            .filter(|row| !row.is_empty());

        std::iter::from_fn(move || {
            let row = rows.next()?;
            let mut cols = row.split(|&b| b == delimiter);
            let id = decode_hex(cols.next().expect("Missing id")).expect("Failed to parse id");
            let pubkey =
                decode_hex(cols.next().expect("Missing pubkey")).expect("Failed to parse pubkey");
            let created_at = atoi::atoi(cols.next().expect("Missing created_at"))
                .expect("Failed to parse created_at");
            let kind =
                atoi::atoi(cols.next().expect("Missing kind")).expect("Failed to parse kind");

            let raw_tags = cols.next().expect("Missing tags");
            let tags_bytes = escape_input(raw_tags);
            let tags = Tags::parse_from_bytes(&tags_bytes);

            let event = EventRow {
                id,
                pubkey,
                created_at,
                kind,
                tags,
            };
            Some(event)
        })
    }
}

fn escape_input(input: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        let b = input[i];

        if b == b'\\' && i + 1 < input.len() {
            let esc = input[i + 1];
            match esc {
                b'n' => res.push(b'\n'),
                b'r' => res.push(b'\r'),
                b't' => res.push(b'\t'),
                b'b' => res.push(8),
                b'f' => res.push(12),
                b'\\' => res.push(b'\\'),
                _ => res.push(esc),
            }
            i += 2;
        } else {
            res.push(b);
            i += 1;
        }
    }

    res
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
