pub const INTERN_FORWARD_CF: &str = "intern_forward";
pub const INTERN_REVERSE_CF: &str = "intern_reverse";
pub const EVENTS_CF: &str = "events";
pub const REPLACEABLE_CF: &str = "replaceable";
pub const ADDRESSABLE_CF: &str = "addressable";
pub const CHECKPOINTS_CF: &str = "checkpoints";

pub const COLUMN_FAMILIES: &[&str] = &[
    INTERN_FORWARD_CF,
    INTERN_REVERSE_CF,
    EVENTS_CF,
    REPLACEABLE_CF,
    ADDRESSABLE_CF,
    CHECKPOINTS_CF,
];

macro_rules! cf {
    ($db:expr, $cf_const:ident) => {{
        $db.cf_handle($cf_const)
            .unwrap_or_else(|| panic!("missing {}", stringify!($cf_const)))
    }};
}
pub(crate) use cf;
