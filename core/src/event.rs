use std::fmt::Debug;

use blake3::hash;
use compact_bytes::CompactBytes;
use serde::{Deserialize, Serialize};

use crate::types::{Bytes32, Node};

pub type Kind = u16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub id: Bytes32,
    pub pubkey: Bytes32,
    pub created_at: u64,
    pub kind: Kind,
    pub tags: Tags,
}

impl EventRow {
    pub fn to_edges(&self) -> Vec<(Node, Node, EdgeLabel)> {
        let id = self.id;
        let author = self.pubkey;

        let mut edges = Vec::with_capacity(1 + self.tags.0.len());
        edges.push((author, id, EdgeLabel::Event));

        for tag in &self.tags.0 {
            let label = EdgeLabel::from(tag);
            match tag {
                Tag::Event { id, .. } => {
                    edges.push((author, *id, label));
                }
                Tag::EventReport(id) => {
                    edges.push((author, *id, label));
                }
                Tag::AddressableEvent(coordinate) => {
                    edges.push((author, coordinate.pubkey, label));
                }
                Tag::Pubkey(pubkey) => {
                    edges.push((author, *pubkey, label));
                }
                Tag::PubkeyReport(pubkey) => {
                    edges.push((author, *pubkey, label));
                }
                Tag::Quote(id) => {
                    edges.push((author, *id, label));
                }
                Tag::QuoteAddress(coordinate) => {
                    edges.push((author, coordinate.pubkey, label));
                }
                Tag::Hashtag(hashtag) => {
                    let hashtag_id = parse_str_to_hex(hashtag.as_slice());
                    edges.push((author, hashtag_id, label));
                }
                Tag::Bolt11(bolt11) => {
                    let bolt11_id = parse_str_to_hex(bolt11.as_slice());
                    edges.push((author, bolt11_id, label));
                }
            }
        }
        edges
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EdgeLabel {
    Hashtag,
    Event,
    EventReport,
    Pubkey,
    PubkeyReport,
    Quote,
    QuoteAddress,
    Bolt11,
}

impl From<&Tag> for EdgeLabel {
    fn from(tag: &Tag) -> Self {
        match tag {
            Tag::Hashtag(_) => EdgeLabel::Hashtag,
            Tag::Event { .. } => EdgeLabel::Event,
            Tag::EventReport(_) => EdgeLabel::EventReport,
            Tag::AddressableEvent(_) => EdgeLabel::Event,
            Tag::Pubkey(_) => EdgeLabel::Pubkey,
            Tag::PubkeyReport(_) => EdgeLabel::PubkeyReport,
            Tag::Quote(_) => EdgeLabel::Quote,
            Tag::QuoteAddress(_) => EdgeLabel::QuoteAddress,
            Tag::Bolt11(_) => EdgeLabel::Bolt11,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Marker {
    Root,
    Reply,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Tag {
    Hashtag(CompactBytes),
    Event { id: Bytes32, marker: Option<Marker> },
    AddressableEvent(Coordinate),
    EventReport(Bytes32),
    Pubkey(Bytes32),
    PubkeyReport(Bytes32),
    Quote(Bytes32),
    QuoteAddress(Coordinate),
    Bolt11(CompactBytes),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tags(Vec<Tag>);

impl Tags {
    pub fn parse_from_bytes(bytes: &[u8]) -> Self {
        let parsed: Vec<Vec<String>> = match serde_json::from_slice(bytes) {
            Ok(v) => v,
            Err(e) => panic!(
                "Failed to parse tags from bytes: {}\n error: {:?}",
                String::from_utf8_lossy(bytes),
                e
            ),
        };

        let tags = parsed
            .iter()
            .filter_map(|tag| {
                let tag_name = tag.first()?;
                let value = tag.get(1)?;

                match tag_name.as_str() {
                    "e" | "E" => decode_hex(value.as_bytes()).map(|id| {
                        let marker = match tag.get(3).map(String::as_str) {
                            Some("root") => Some(Marker::Root),
                            Some("reply") => Some(Marker::Reply),
                            _ => Some(Marker::Root),
                        };
                        Tag::Event { id, marker }
                    }),
                    "a" | "A" => Coordinate::parse_address(value).map(Tag::AddressableEvent),
                    "p" | "P" => decode_hex(value.as_bytes()).map(Tag::Pubkey),
                    "q" => {
                        if value.as_bytes().contains(&b':') {
                            Coordinate::parse_address(value).map(Tag::QuoteAddress)
                        } else {
                            decode_hex(value.as_bytes()).map(Tag::Quote)
                        }
                    }
                    "t" => Some(Tag::Hashtag(CompactBytes::new(value.as_bytes()))),
                    "bolt11" => Some(Tag::Bolt11(CompactBytes::new(value.as_bytes()))),
                    _ => None,
                }
            })
            .collect();

        Tags(tags)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Coordinate {
    pubkey: Bytes32,
    address: Bytes32,
}

impl Coordinate {
    pub fn parse_address(value: &str) -> Option<Self> {
        let mut parts = value.splitn(3, ':');

        let _ = parts.next()?; // kind
        let pubkey_part = parts.next()?;
        let _ = parts.next()?; // identifier

        // People are putting all sorts of crap in nostr tags.
        if pubkey_part.len() != 64 {
            return None;
        }

        let pubkey = decode_hex(pubkey_part.as_bytes())?;
        let address = parse_str_to_hex(value.as_bytes());

        Some(Self { pubkey, address })
    }
}

pub fn parse_str_to_hex(value: &[u8]) -> [u8; 32] {
    *hash(value).as_bytes()
}

pub fn decode_hex(value: &[u8]) -> Option<Bytes32> {
    // Make sure to remove all crap from e and p tags.
    if value.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice::<_>(value, &mut out).ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_parse_str_to_hex() {
        let coordinate =
            b"30023:c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e:my-article";
        let a = parse_str_to_hex(coordinate);
        assert_eq!(
            hex::encode(a),
            "4f081fec199864934b6ad2142c4c6880479e1ad850b45ba4a13090bedacc8e01"
        );
    }

    #[test]
    fn assert_decode_hex() {
        let pubkey = "c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e".to_owned();
        let result = decode_hex(pubkey.as_bytes());
        let expected = Some([
            198, 96, 59, 15, 28, 207, 236, 98, 93, 156, 8, 183, 83, 228, 247, 116, 234, 247, 209,
            207, 39, 105, 34, 49, 37, 181, 253, 77, 167, 40, 1, 158,
        ]);
        assert_eq!(result, expected);
        assert_eq!(hex::encode(result.unwrap()), pubkey);
    }

    #[test]
    fn assert_invalid_hex() {
        let pubkey = "invalid_pubkey".to_owned();
        let bytes = decode_hex(pubkey.as_bytes());
        assert!(bytes.is_none());
    }

    #[test]
    fn assert_coordinates() {
        let address =
            "30023:c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e:article2";
        let coordinate = Coordinate::parse_address(address).unwrap();
        assert_eq!(
            hex::encode(coordinate.pubkey),
            "c6603b0f1ccfec625d9c08b753e4f774eaf7d1cf2769223125b5fd4da728019e"
        );
    }

    #[test]
    fn assert_invalid_coordinates() {
        assert!(Coordinate::parse_address("[]").is_none());
        assert!(Coordinate::parse_address("1:pubkey").is_none());
    }
}
