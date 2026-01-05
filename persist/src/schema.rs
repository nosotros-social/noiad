pub const INTERN_FORWARD: &str = "intern_forward";
pub const INTERN_REVERSE: &str = "intern_reverse";
pub const EVENTS: &str = "events";
pub const EVENTS_BY_PUBKEY: &str = "events_by_pubkey";
pub const CHECKPOINTS: &str = "checkpoints";

pub const COLUMN_FAMILIES: &[&str] = &[
    INTERN_FORWARD,
    INTERN_REVERSE,
    EVENTS,
    EVENTS_BY_PUBKEY,
    CHECKPOINTS,
];

macro_rules! cf {
    ($db:expr, $cf_const:ident) => {{
        $db.cf_handle($cf_const)
            .unwrap_or_else(|| panic!("missing {}", stringify!($cf_const)))
    }};
}
pub(crate) use cf;
