use types::event::EventRaw;

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

    pub fn iter_rows(&self) -> impl Iterator<Item = EventRaw> + '_ {
        self.data
            .split(|&b| b == b'\n')
            .filter(|row| !row.is_empty())
            .filter_map(|row| parse_copy_row(row, self.column_delimiter))
    }
}

fn parse_copy_row(row: &[u8], delimiter: u8) -> Option<EventRaw> {
    let mut cols = row.split(|&b| b == delimiter);

    let id_hex = cols.next()?;
    let pubkey_hex = cols.next()?;
    let created_at = atoi::atoi::<u64>(cols.next()?)?;
    let kind = atoi::atoi::<u16>(cols.next()?)?;
    let tags_input = cols.next()?;

    let mut id = [0u8; 32];
    let mut pubkey = [0u8; 32];
    hex::decode_to_slice(id_hex, &mut id).ok()?;
    hex::decode_to_slice(pubkey_hex, &mut pubkey).ok()?;

    let mut tags_json = Vec::with_capacity(tags_input.len());
    escape_into(tags_input, &mut tags_json);

    Some(EventRaw {
        id,
        pubkey,
        created_at,
        kind,
        tags_json,
    })
}

fn escape_into(input: &[u8], output: &mut Vec<u8>) {
    output.reserve(input.len().saturating_sub(output.capacity()));

    let mut i = 0;
    while i < input.len() {
        let b = input[i];

        if b == b'\\' && i + 1 < input.len() {
            let esc = input[i + 1];
            match esc {
                b'n' => output.push(b'\n'),
                b'r' => output.push(b'\r'),
                b't' => output.push(b'\t'),
                b'b' => output.push(8),
                b'f' => output.push(12),
                b'\\' => output.push(b'\\'),
                _ => output.push(esc),
            }
            i += 2;
        } else {
            output.push(b);
            i += 1;
        }
    }
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
