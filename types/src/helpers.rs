#[macro_export]
macro_rules! event_row {
    ($($field:ident : $value:expr),* $(,)?) => {
        EventRow {
            $($field: $value,)*
                ..Default::default()
        }
    };
}
