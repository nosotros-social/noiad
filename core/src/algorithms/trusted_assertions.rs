use std::str::FromStr;

use differential_dataflow::operators::join::Join;
use differential_dataflow::{
    VecCollection,
    lattice::Lattice,
    operators::{CountTotal, Reduce, Threshold},
};
use lightning_invoice::Bolt11Invoice;
use nostr_sdk::Kind;
use serde::{Deserialize, Serialize};
use timely::{dataflow::Scope, order::TotalOrder};

use crate::{
    event::{EdgeLabel, EventRow, Tag},
    types::{Diff, Node},
};

macro_rules! trust_field {
    ($field:ident) => {
        TrustedAssertion {
            $field, // Rust handles the mapping automatically if variable name == field name
            ..Default::default()
        }
    };
    ($field:ident : $val:expr) => {
        TrustedAssertion {
            $field: $val,
            ..Default::default()
        }
    };
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TrustedAssertion {
    pub follower_cnt: Diff,
    pub rank: Option<Diff>,
    pub first_created_at: Option<u64>,
    pub post_cnt: Diff,
    pub reply_cnt: Diff,
    pub reactions_cnt: Diff,
    pub zap_amt_recd: Diff,
    pub zap_amt_sent: Diff,
    pub zap_cnt_recd: Diff,
    pub zap_cnt_sent: Diff,
    pub zap_avg_amt_day_recd: Diff,
    pub zap_avg_amt_day_sent: Diff,
    pub reports_cnt_sent: Diff,
    pub reports_cnt_recd: Diff,
    pub topics: Vec<(Vec<u8>, Diff)>,
    pub active_hours_start: Option<u8>,
    pub active_hours_end: Option<u8>,
}

const MIN_NORMALIZED_RANK: Diff = 10;

/// Parameters for pagerank normalization curve
/// https://gist.github.com/pippellia-btc/8642a25fcf535edcda1ddecd0bcd5f7b
const PAGERANK_EXPONENT: f64 = 0.76;
const SCORE_CURVE_EXPONENT: f64 = 0.38;
const SCORE_BASELINE_OFFSET: f64 = 1.0 - PAGERANK_EXPONENT;
const AVERAGE_RANK: f64 = 6_000_000.0;

/// Our pagerank implementation each node starts with 6,000,000 "surfers",
/// so we use that as the average rank for normalization.
pub fn normalize_pagerank(rank: Diff) -> Diff {
    if rank <= 0 {
        return 0;
    }

    let rank_f = rank as f64;

    let relative_strength = rank_f / AVERAGE_RANK;
    let denominator = relative_strength + SCORE_BASELINE_OFFSET;
    let value = 1.0 - (SCORE_BASELINE_OFFSET / denominator).powf(SCORE_CURVE_EXPONENT);

    let score = (value * 100.0).clamp(0.0, 100.0);
    score.round() as Diff
}

/// NIP-85 implementation
/// https://github.com/nostr-protocol/nips/blob/1ced632b45f603aca46a4741b54beea1b090388c/85.md
pub fn trusted_assertions<G>(
    events: &VecCollection<G, EventRow, Diff>,
    edges: &VecCollection<G, (Kind, Node, Node, EdgeLabel), Diff>,
    ranks: &VecCollection<G, Node, Diff>,
) -> VecCollection<G, (Node, TrustedAssertion), Diff>
where
    G: Scope,
    G::Timestamp: Lattice + TotalOrder,
{
    let rank_totals = ranks.count_total();
    // ranks from 0 to 100
    let normalized_ranks = rank_totals.map(|(pk, rank)| (pk, normalize_pagerank(rank)));

    let follower_cnt = edges
        .filter(|&(kind, _, _, _)| kind == Kind::ContactList)
        .filter(|&(_, _, _, label)| label == EdgeLabel::Pubkey)
        .map(|(_, follower, followed, _)| (follower, followed))
        // attach follower rank
        .join_map(&normalized_ranks, |_follower, &followed, &rank| {
            (followed, rank)
        })
        // keep only followers above rank threshold
        .filter(|&(_followed, rank)| rank >= MIN_NORMALIZED_RANK)
        .map(|(followed, _rank)| followed)
        .distinct()
        .count_total()
        .map(|(pk, follower_cnt)| (pk, trust_field!(follower_cnt)));

    let rank = normalized_ranks.map(|(pk, rank)| (pk, trust_field!(rank: Some(rank))));

    let first_post_time = events
        .map(|e| (e.pubkey, e.created_at))
        .reduce(|_pk, times, output| {
            let mut min_time: Option<u64> = None;

            for &(&created_at, diff) in times.iter() {
                if diff <= 0 {
                    continue;
                }

                min_time = match min_time {
                    None => Some(created_at),
                    Some(cur) if created_at < cur => Some(created_at),
                    Some(cur) => Some(cur),
                };
            }

            if let Some(ts) = min_time {
                output.push((ts, 1));
            }
        })
        .map(|(pk, ts)| (pk, trust_field!(first_created_at: Some(ts))));

    let post_cnt = events
        .filter(|e| e.is_root_post())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, post_cnt)| (pk, trust_field!(post_cnt)));

    let reply_cnt = events
        .filter(|e| e.is_reply())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, reply_cnt)| (pk, trust_field!(reply_cnt)));

    let reactions_cnt = events
        .filter(|e| e.kind == Kind::Reaction)
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, reactions_cnt)| (pk, trust_field!(reactions_cnt)));

    let zap_events = events.filter(|e| e.kind == Kind::ZapReceipt).flat_map(|e| {
        let day = e.created_at / 86_400;

        let mut sender: Option<Node> = None;
        let mut recipient: Option<Node> = None;
        let mut amount: Option<Diff> = None;

        for tag in &e.tags.0 {
            match tag {
                Tag::Pubkey {
                    pubkey,
                    uppercase: true,
                } => {
                    if sender.is_none() {
                        sender = Some(*pubkey);
                    }
                }
                Tag::Pubkey {
                    pubkey,
                    uppercase: false,
                } => {
                    if recipient.is_none() {
                        recipient = Some(*pubkey);
                    }
                }
                Tag::Bolt11(bolt) => {
                    if amount.is_none()
                        && let Ok(s) = std::str::from_utf8(bolt.as_slice())
                    {
                        amount = parse_bolt11_amount_sats(s);
                    }
                }
                _ => {}
            }
        }

        let amount = match amount {
            Some(a) => a,
            None => return None,
        };

        Some((sender, recipient, amount, day))
    });

    let zap_amt_sent = zap_events
        .flat_map(|(sender_opt, _recipient_opt, amount, _day)| {
            sender_opt.map(|sender| (sender, amount))
        })
        .reduce(|_pk, vals, output| {
            let mut total: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total += *amt * *diff;
            }

            if total != 0 {
                output.push((total, 1));
            }
        })
        .map(|(pk, zap_amt_sent)| (pk, trust_field!(zap_amt_sent)));

    let zap_amt_recd = zap_events
        .flat_map(|(_sender, recipient_opt, amount, _day)| {
            recipient_opt.map(|recipient| (recipient, amount))
        })
        .reduce(|_pk, vals, output| {
            let mut total: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total += *amt * *diff;
            }

            if total != 0 {
                output.push((total, 1));
            }
        })
        .map(|(pk, zap_amt_recd)| (pk, trust_field!(zap_amt_recd)));

    let zap_cnt_sent = zap_events
        .flat_map(|(sender, _receiver, _amount, _day)| sender)
        .count_total()
        .map(|(pk, zap_cnt_sent)| (pk, trust_field!(zap_cnt_sent)));

    let zap_cnt_recd = zap_events
        .flat_map(|(_sender, recipient, _amount, _day)| recipient)
        .count_total()
        .map(|(pk, zap_cnt_recd)| (pk, trust_field!(zap_cnt_recd)));

    let zap_sent_by_day = zap_events
        .flat_map(|(sender_opt, _recipient_opt, amount, day)| {
            sender_opt.map(|sender| ((sender, day), amount))
        })
        .reduce(|_key, vals, output| {
            let mut total: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total += *amt * *diff;
            }

            if total != 0 {
                output.push((total, 1));
            }
        });

    let zap_avg_amt_day_sent = zap_sent_by_day
        .map(|((sender, _day), amount)| (sender, amount))
        .reduce(|_pk, vals, output| {
            let mut total: Diff = 0;
            let mut days: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total += *amt * *diff;
                days += *diff;
            }

            if days > 0 {
                let avg = total / days;
                if avg != 0 {
                    output.push((avg, 1));
                }
            }
        })
        .map(|(pk, zap_avg_amt_day_sent)| (pk, trust_field!(zap_avg_amt_day_sent)));

    let zap_recd_by_day = zap_events
        .flat_map(|(_sender_opt, recipient_opt, amount, day)| {
            recipient_opt.map(|recipient| ((recipient, day), amount))
        })
        .reduce(|_key, vals, output| {
            let mut total: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total += *amt * *diff;
            }

            if total != 0 {
                output.push((total, 1));
            }
        });

    let zap_avg_amt_day_recd = zap_recd_by_day
        .map(|((recipient, _day), amount)| (recipient, amount))
        .reduce(|_pk, vals, output| {
            let mut total: Diff = 0;
            let mut days: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total += *amt * *diff;
                days += *diff;
            }

            if days > 0 {
                let avg = total / days;
                if avg != 0 {
                    output.push((avg, 1));
                }
            }
        })
        .map(|(pk, zap_avg_amt_day_recd)| (pk, trust_field!(zap_avg_amt_day_recd)));

    let reports_cnt_sent = events
        .filter(|e| e.has_report())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, reports_cnt_sent)| (pk, trust_field!(reports_cnt_sent)));

    let reports_cnt_recd = events
        .flat_map(|e| e.reported_pubkeys())
        .count_total()
        .map(|(pk, reports_cnt_recd)| (pk, trust_field!(reports_cnt_recd)));

    let topic_counts = events
        .flat_map(|e| e.hashtags_for_author())
        .map(|(pk, topic)| (pk, topic.as_slice().to_vec()))
        .count_total()
        .map(|((pk, topic_bytes), count)| (pk, (topic_bytes, count)))
        .map(|(pk, (topic_bytes, count))| {
            let mut ta = TrustedAssertion::default();
            ta.topics.push((topic_bytes, count));
            (pk, ta)
        });

    let by_hour = events.map(|e| {
        let hour = ((e.created_at / 3600) % 24) as u8;
        (e.pubkey, hour)
    });

    let active_hours_start = by_hour
        .reduce(|_pk, hours, output| {
            let mut min_hour: Option<u8> = None;

            for &(h, diff) in hours.iter() {
                if diff <= 0 {
                    continue;
                }

                let hour = *h;

                min_hour = match min_hour {
                    None => Some(hour),
                    Some(cur) if hour < cur => Some(hour),
                    Some(cur) => Some(cur),
                };
            }

            if let Some(h) = min_hour {
                output.push((h, 1));
            }
        })
        .map(|(pk, h)| (pk, trust_field!(active_hours_start: Some(h))));

    let active_hours_end = by_hour
        .reduce(|_pk, hours, output| {
            let mut max_hour: Option<u8> = None;

            for &(h, diff) in hours.iter() {
                if diff <= 0 {
                    continue;
                }

                let hour = *h;

                max_hour = match max_hour {
                    None => Some(hour),
                    Some(cur) if hour > cur => Some(hour),
                    Some(cur) => Some(cur),
                };
            }

            if let Some(h) = max_hour {
                output.push((h, 1));
            }
        })
        .map(|(pk, h)| (pk, trust_field!(active_hours_end: Some(h))));

    let parts = follower_cnt
        .concat(&first_post_time)
        .concat(&rank)
        .concat(&post_cnt)
        .concat(&reply_cnt)
        .concat(&reactions_cnt)
        .concat(&zap_cnt_recd)
        .concat(&zap_cnt_sent)
        .concat(&zap_amt_recd)
        .concat(&zap_amt_sent)
        .concat(&zap_avg_amt_day_recd)
        .concat(&zap_avg_amt_day_sent)
        .concat(&reports_cnt_sent)
        .concat(&reports_cnt_recd)
        .concat(&topic_counts)
        .concat(&active_hours_start)
        .concat(&active_hours_end);

    // Merge all parts into a single TrustedAssertion per pubkey
    parts.reduce(|_pk, parts, output| {
        let mut agg = TrustedAssertion::default();

        for (row, diff) in parts.iter() {
            if *diff <= 0 {
                continue;
            }

            let r = row;

            if r.follower_cnt != 0 {
                agg.follower_cnt = r.follower_cnt;
            }

            if let Some(ts) = r.first_created_at {
                agg.first_created_at = match agg.first_created_at {
                    None => Some(ts),
                    Some(cur) => Some(cur.min(ts)),
                };
            }

            if r.post_cnt != 0 {
                agg.post_cnt = r.post_cnt;
            }

            if r.reply_cnt != 0 {
                agg.reply_cnt = r.reply_cnt;
            }

            if r.reactions_cnt != 0 {
                agg.reactions_cnt = r.reactions_cnt;
            }

            if r.zap_amt_recd != 0 {
                agg.zap_amt_recd = r.zap_amt_recd;
            }

            if r.zap_amt_sent != 0 {
                agg.zap_amt_sent = r.zap_amt_sent;
            }

            if r.zap_cnt_recd != 0 {
                agg.zap_cnt_recd = r.zap_cnt_recd;
            }

            if r.zap_cnt_sent != 0 {
                agg.zap_cnt_sent = r.zap_cnt_sent;
            }

            if r.zap_avg_amt_day_recd != 0 {
                agg.zap_avg_amt_day_recd = r.zap_avg_amt_day_recd;
            }

            if r.zap_avg_amt_day_sent != 0 {
                agg.zap_avg_amt_day_sent = r.zap_avg_amt_day_sent;
            }

            if r.reports_cnt_sent != 0 {
                agg.reports_cnt_sent = r.reports_cnt_sent;
            }

            if r.reports_cnt_recd != 0 {
                agg.reports_cnt_recd = r.reports_cnt_recd;
            }

            if !r.topics.is_empty() {
                agg.topics.extend(r.topics.iter().cloned());
            }

            if let Some(h) = r.active_hours_start {
                agg.active_hours_start = match agg.active_hours_start {
                    None => Some(h),
                    Some(cur) => Some(cur.min(h)),
                };
            }

            if let Some(h) = r.active_hours_end {
                agg.active_hours_end = match agg.active_hours_end {
                    None => Some(h),
                    Some(cur) => Some(cur.max(h)),
                };
            }

            if let Some(rank) = r.rank {
                agg.rank = Some(rank);
            }
        }

        output.push((agg, 1));
    })
}

fn parse_bolt11_amount_sats(bolt11: &str) -> Option<Diff> {
    let invoice = Bolt11Invoice::from_str(bolt11).ok()?;
    let msat = invoice.amount_milli_satoshis()?;
    let sats = msat / 1000;
    Some(sats as Diff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use compact_bytes::CompactBytes;
    use differential_dataflow::input::InputSession;
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::execute_directly;

    use crate::algorithms::pagerank::pagerank;
    use crate::event::{EventRow, Marker, Tag, Tags};
    use crate::types::{Diff, Node};

    const PUBKEY1: Node = [1u8; 32];
    const PUBKEY2: Node = [2u8; 32];
    const PUBKEY3: Node = [3u8; 32];
    const PUBKEY4: Node = [4u8; 32];
    const PUBKEY5: Node = [5u8; 32];
    const PUBKEY_LN: Node = [9u8; 32];

    use core::time::Duration;
    use lightning::bitcoin::hashes::{Hash as _, sha256};
    use lightning::bitcoin::secp256k1::{Secp256k1, SecretKey};
    use lightning::bolt11_invoice::{Currency, InvoiceBuilder};
    use lightning::types::payment::{PaymentHash, PaymentSecret};
    use std::sync::mpsc::channel;

    fn build_bolt11(sats: u64) -> String {
        let secp = Secp256k1::new();
        let node_secret_key = SecretKey::from_slice(&[42u8; 32]).unwrap();
        let payment_hash = PaymentHash([0u8; 32]);
        let payment_secret = PaymentSecret([1u8; 32]);
        let payment_hash_sha256 = sha256::Hash::from_slice(&payment_hash.0).unwrap();
        let amount_msat = sats * 1000;

        let builder = InvoiceBuilder::new(Currency::Bitcoin)
            .description("test zap".to_string())
            .payment_hash(payment_hash_sha256)
            .payment_secret(payment_secret)
            .duration_since_epoch(Duration::from_secs(1_700_000_000))
            .min_final_cltv_expiry_delta(144)
            .amount_milli_satoshis(amount_msat);

        let invoice = builder
            .build_signed(|msg| secp.sign_ecdsa_recoverable(msg, &node_secret_key))
            .unwrap();

        invoice.to_string()
    }

    fn build_dataflow<FBuild>(build_inputs: FBuild) -> Vec<(Node, TrustedAssertion, u64, Diff)>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRow, Diff>) + Send + Sync + 'static,
    {
        let (tx, rx) = channel();

        execute_directly(move |worker| {
            let mut events_input: InputSession<u64, EventRow, Diff> = InputSession::new();
            let probe = ProbeHandle::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let events = events_input.to_collection(scope);
                let edges = events.flat_map(|e| e.to_edges());
                let ranks = pagerank(
                    20,
                    &edges
                        .filter(|&(kind, _, _, _)| kind == Kind::ContactList)
                        .filter(|&(_, _, _, label)| label == EdgeLabel::Pubkey)
                        .map(|(_, from, to, _)| (from, to)),
                );

                let assertions = trusted_assertions(&events, &edges, &ranks);

                assertions.probe_with(&probe).inner.capture_into(tx);
            });

            events_input.advance_to(1);

            build_inputs(&mut events_input);

            events_input.flush();

            let mut final_ts = *events_input.time();
            final_ts += 1;

            events_input.advance_to(final_ts);
            events_input.flush();

            while probe.less_than(&final_ts) {
                worker.step();
            }
        });

        rx.extract()
            .into_iter()
            .flat_map(|(_cap_ts, batch)| {
                batch
                    .into_iter()
                    .map(|((pubkey, res), ts, diff)| (pubkey, res, ts, diff))
            })
            .collect()
    }

    fn get_captured_by_pubkey(
        captured: &[(Node, TrustedAssertion, u64, Diff)],
        pubkey: Node,
    ) -> Vec<(Node, TrustedAssertion, u64, Diff)> {
        let mut res = captured
            .iter()
            .filter(|(p, _res, _, diff)| *p == pubkey && *diff > 0)
            .cloned()
            .collect::<Vec<_>>();
        res.sort_by_key(|(_, _, ts, _)| *ts);
        res
    }

    #[test]
    fn assert_trusted_assertions_for_pubkey() {
        let captured = build_dataflow(|events_input| {
            // Follow events
            events_input.insert(EventRow {
                id: [30u8; 32],
                pubkey: PUBKEY1,
                created_at: 2 * 3600,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY2,
                    uppercase: false,
                }]),
            });
            events_input.insert(EventRow {
                id: [31u8; 32],
                pubkey: PUBKEY2,
                created_at: 10 * 3600,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY1,
                    uppercase: false,
                }]),
            });
            events_input.insert(EventRow {
                id: [32u8; 32],
                pubkey: PUBKEY3,
                created_at: 7 * 3600,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY1,
                    uppercase: false,
                }]),
            });

            // pubkey1 root posts
            events_input.insert(EventRow {
                id: [10u8; 32],
                pubkey: PUBKEY1,
                created_at: 3600,
                kind: Kind::TextNote,
                tags: Tags(vec![Tag::Hashtag(CompactBytes::new(b"nostr"))]),
            });
            events_input.insert(EventRow {
                id: [11u8; 32],
                pubkey: PUBKEY1,
                created_at: 3 * 3600,
                kind: Kind::TextNote,
                tags: Tags(vec![Tag::Hashtag(CompactBytes::new(b"wot"))]),
            });

            // pubkey1 replies at hour 20
            events_input.insert(EventRow {
                id: [12u8; 32],
                pubkey: PUBKEY1,
                created_at: 20 * 3600,
                kind: Kind::TextNote,
                tags: Tags(vec![Tag::Event {
                    id: [9u8; 32],
                    marker: Some(Marker::Reply),
                }]),
            });

            // pubkey1 reacts to note 10 from pubkey1
            events_input.insert(EventRow {
                id: [13u8; 32],
                pubkey: PUBKEY1,
                created_at: 21 * 3600,
                kind: Kind::Reaction,
                tags: Tags(vec![
                    Tag::Event {
                        id: [10u8; 32],
                        marker: None,
                    },
                    Tag::Pubkey {
                        pubkey: PUBKEY1,
                        uppercase: false,
                    },
                ]),
            });

            // pubkey1 reacts to note 11 from pubkey1
            events_input.insert(EventRow {
                id: [14u8; 32],
                pubkey: PUBKEY1,
                created_at: 22 * 3600,
                kind: Kind::Reaction,
                tags: Tags(vec![
                    Tag::Event {
                        id: [11u8; 32],
                        marker: None,
                    },
                    Tag::Pubkey {
                        pubkey: PUBKEY1,
                        uppercase: false,
                    },
                ]),
            });

            // pubkey1 reports pubkey2
            events_input.insert(EventRow {
                id: [15u8; 32],
                pubkey: PUBKEY1,
                created_at: 23 * 3600,
                kind: Kind::Reporting,
                tags: Tags(vec![Tag::PubkeyReport(PUBKEY2)]),
            });

            events_input.insert(EventRow {
                id: [16u8; 32],
                pubkey: PUBKEY2,
                created_at: 5 * 3600,
                kind: Kind::TextNote,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY1,
                    uppercase: false,
                }]),
            });

            events_input.insert(EventRow {
                id: [17u8; 32],
                pubkey: PUBKEY3,
                created_at: 6 * 3600,
                kind: Kind::TextNote,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY1,
                    uppercase: false,
                }]),
            });

            events_input.insert(EventRow {
                id: [18u8; 32],
                pubkey: PUBKEY3,
                created_at: 7 * 3600,
                kind: Kind::TextNote,
                tags: Tags(vec![]),
            });

            events_input.insert(EventRow {
                id: [19u8; 32],
                pubkey: PUBKEY3,
                created_at: 8 * 3600,
                kind: Kind::Reaction,
                tags: Tags(vec![]),
            });

            // pubkey1 zaps pubkey2 1000 sats
            events_input.insert(EventRow {
                id: [20u8; 32],
                pubkey: PUBKEY_LN,
                created_at: 9 * 3600,
                kind: Kind::ZapReceipt,
                tags: Tags(vec![
                    Tag::Pubkey {
                        pubkey: PUBKEY2,
                        uppercase: false,
                    },
                    Tag::Pubkey {
                        pubkey: PUBKEY1,
                        uppercase: true,
                    },
                    Tag::Bolt11(CompactBytes::new(build_bolt11(1000).as_bytes())),
                ]),
            });
            // pubkey2 zaps pubkey1 2000 sats
            events_input.insert(EventRow {
                id: [21u8; 32],
                pubkey: PUBKEY_LN,
                created_at: 10 * 3600,
                kind: Kind::ZapReceipt,
                tags: Tags(vec![
                    Tag::Pubkey {
                        pubkey: PUBKEY1,
                        uppercase: false,
                    },
                    Tag::Pubkey {
                        pubkey: PUBKEY2,
                        uppercase: true,
                    },
                    Tag::Bolt11(CompactBytes::new(build_bolt11(2000).as_bytes())),
                ]),
            });
        });

        let get_assertion = |pubkey: Node| {
            get_captured_by_pubkey(&captured, pubkey)
                .into_iter()
                .next()
                .map(|(_, res, _, _)| res)
                .unwrap()
        };

        let res1 = get_assertion(PUBKEY1);
        let res2 = get_assertion(PUBKEY2);
        let res3 = get_assertion(PUBKEY3);

        // pubkey1
        {
            let mut topics = res1.topics.clone();
            topics.sort();

            let mut expected_topics = vec![(b"wot".to_vec(), 1), (b"nostr".to_vec(), 1)];
            expected_topics.sort();

            let expected = TrustedAssertion {
                follower_cnt: 1,
                rank: Some(52),
                first_created_at: Some(3600),
                post_cnt: 2,
                reply_cnt: 1,
                reactions_cnt: 2,
                zap_amt_sent: 1_000,
                zap_cnt_sent: 1,
                zap_avg_amt_day_sent: 1_000,
                zap_amt_recd: 2_000,
                zap_cnt_recd: 1,
                zap_avg_amt_day_recd: 2_000,
                reports_cnt_sent: 1,
                reports_cnt_recd: 0,
                topics: expected_topics.clone(),
                active_hours_start: Some(1),
                active_hours_end: Some(23),
            };

            let mut res1_sorted = res1.clone();
            res1_sorted.topics = topics;

            assert_eq!(expected_topics, res1_sorted.topics);
            assert_eq!(expected, res1_sorted);
        }

        // pubkey2
        {
            let expected = TrustedAssertion {
                follower_cnt: 1,
                rank: Some(52),
                first_created_at: Some(5 * 3600),
                post_cnt: 1,
                reply_cnt: 0,
                reactions_cnt: 0,
                zap_amt_sent: 2_000,
                zap_cnt_sent: 1,
                zap_avg_amt_day_sent: 2_000,
                zap_amt_recd: 1_000,
                zap_cnt_recd: 1,
                zap_avg_amt_day_recd: 1_000,
                reports_cnt_sent: 0,
                reports_cnt_recd: 1,
                topics: vec![],
                active_hours_start: Some(5),
                active_hours_end: Some(10),
            };

            assert_eq!(expected, res2);
        }

        // pubkey3
        {
            let expected = TrustedAssertion {
                first_created_at: Some(21600),
                rank: Some(18),
                post_cnt: 2,
                reactions_cnt: 1,
                active_hours_start: Some(6),
                active_hours_end: Some(8),
                ..Default::default()
            };

            assert_eq!(expected, res3);
        }
    }

    #[test]
    fn assert_follow_edges_inserts_and_deletes() {
        let captured = build_dataflow(|events_input| {
            events_input.insert(EventRow {
                id: [1u8; 32],
                pubkey: PUBKEY2,
                created_at: 2000,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY1,
                    uppercase: false,
                }]),
            });

            events_input.insert(EventRow {
                id: [2u8; 32],
                pubkey: PUBKEY1,
                created_at: 2000,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY2,
                    uppercase: false,
                }]),
            });

            events_input.insert(EventRow {
                id: [3u8; 32],
                pubkey: PUBKEY3,
                created_at: 2000,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY4,
                    uppercase: false,
                }]),
            });
            events_input.insert(EventRow {
                id: [4u8; 32],
                pubkey: PUBKEY4,
                created_at: 2000,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY5,
                    uppercase: false,
                }]),
            });
            events_input.flush();

            // pubkey1 unfollows pubkey2 at timestamp 2
            events_input.advance_to(2);
            events_input.remove(EventRow {
                id: [2u8; 32],
                pubkey: PUBKEY1,
                created_at: 2000,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY2,
                    uppercase: false,
                }]),
            });
            events_input.flush();

            // pubkey1 follows pubkey2 again at timestamp 3
            events_input.advance_to(3);

            events_input.insert(EventRow {
                id: [4u8; 32],
                pubkey: PUBKEY1,
                created_at: 4000,
                kind: Kind::ContactList,
                tags: Tags(vec![Tag::Pubkey {
                    pubkey: PUBKEY2,
                    uppercase: false,
                }]),
            });

            events_input.flush();
        });

        let get_rank_from_pubkey = |pubkey: Node| {
            get_captured_by_pubkey(&captured, pubkey)
                .iter()
                .map(|(_, res, ts, _)| (*ts, res.rank.unwrap()))
                .collect::<Vec<_>>()
        };
        let pubkey1_data = get_rank_from_pubkey(PUBKEY1);
        let pubkey2_data = get_rank_from_pubkey(PUBKEY2);
        let pubkey3_data = get_rank_from_pubkey(PUBKEY3);
        let pubkey4_data = get_rank_from_pubkey(PUBKEY4);

        // (timestamp, rank)
        assert_eq!(pubkey1_data, vec![(1, 46), (2, 27), (3, 46)]);
        assert_eq!(pubkey2_data, vec![(1, 46), (2, 18), (3, 46)]);
        // It's important to make sure that pubkey3 and pubkey4 ranks are unchanged
        assert_eq!(pubkey3_data, vec![(1, 18)]);
        assert_eq!(pubkey4_data, vec![(1, 27)]);
    }

    #[test]
    fn assert_normalize_pagerank() {
        assert_eq!(normalize_pagerank(0 as Diff), 0);
        assert_eq!(normalize_pagerank(-1 as Diff), 0);
        assert_eq!(normalize_pagerank(1_000_000 as Diff), 18);
        assert_eq!(normalize_pagerank(3_000_000 as Diff), 35);
        assert_eq!(normalize_pagerank(6_000_000 as Diff), 46);
        assert_eq!(normalize_pagerank(12_000_000 as Diff), 57);
        assert_eq!(normalize_pagerank(21_000_000 as Diff), 65);
        assert_eq!(normalize_pagerank(120_000_000 as Diff), 81);
        assert_eq!(normalize_pagerank(600_000_000 as Diff), 90);
    }

    const PUBKEY_NO_EVENTS: Node = [7u8; 32];

    #[test]
    fn assert_pubkey_without_events() {
        let captured = build_dataflow(|events_input| {
            events_input.insert(EventRow {
                id: [42u8; 32],
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::Reporting,
                tags: Tags(vec![Tag::PubkeyReport(PUBKEY_NO_EVENTS)]),
            });
        });

        let rows = get_captured_by_pubkey(&captured, PUBKEY_NO_EVENTS);
        assert_eq!(rows.len(), 1);

        let (pk, assertion, _, _) = &rows[0];

        let expected = TrustedAssertion {
            reports_cnt_recd: 1,
            first_created_at: None,
            ..Default::default()
        };

        assert_eq!(*pk, PUBKEY_NO_EVENTS);
        assert_eq!(*assertion, expected);
    }
}
