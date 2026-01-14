use differential_dataflow::operators::Threshold;
use differential_dataflow::operators::join::Join;
use differential_dataflow::{
    VecCollection,
    lattice::Lattice,
    operators::{CountTotal, Reduce},
};
use nostr_sdk::Kind;
use persist::event::EventRecord;
use persist::tag::EventTag;
use serde::{Deserialize, Serialize};
use timely::{dataflow::Scope, order::TotalOrder};
use types::event::{Edge, Node};

use crate::types::Diff;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum TrustedPart {
    FollowerCnt(Diff),
    Rank(Diff),
    FirstCreatedAt(u64),
    PostCnt(Diff),
    ReplyCnt(Diff),
    ReactionsCnt(Diff),
    ZapAmtRecd(u64),
    ZapAmtSent(u64),
    ZapCntRecd(Diff),
    ZapCntSent(Diff),
    ZapAvgAmtDayRecd(u64),
    ZapAvgAmtDaySent(u64),
    ReportsCntSent(Diff),
    ReportsCntRecd(Diff),
    // Topics(Vec<(u32, Diff)>),
    ActiveHoursStart(u8),
    ActiveHoursEnd(u8),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TrustedAssertion {
    pub follower_cnt: Diff,
    pub rank: Option<Diff>,
    pub first_created_at: Option<u64>,
    pub post_cnt: Diff,
    pub reply_cnt: Diff,
    pub reactions_cnt: Diff,
    pub zap_amt_recd: u64,
    pub zap_amt_sent: u64,
    pub zap_cnt_recd: Diff,
    pub zap_cnt_sent: Diff,
    pub zap_avg_amt_day_recd: u64,
    pub zap_avg_amt_day_sent: u64,
    pub reports_cnt_sent: Diff,
    pub reports_cnt_recd: Diff,
    // pub topics: Vec<(u32, Diff)>,
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
    events: &VecCollection<G, EventRecord, Diff>,
    edges_follows: &VecCollection<G, Edge, Diff>,
    ranks: &VecCollection<G, Node, Diff>,
) -> VecCollection<G, (Node, TrustedAssertion), Diff>
where
    G: Scope,
    G::Timestamp: Lattice + TotalOrder,
{
    let rank_totals = ranks.count_total();
    // ranks from 0 to 100
    let normalized_ranks = rank_totals.map(|(pk, rank)| (pk, normalize_pagerank(rank)));

    let followers_ranked = normalized_ranks
        .filter(|&(_pk, rank)| rank >= MIN_NORMALIZED_RANK)
        .map(|(pk, _rank)| (pk, ()));

    let follower_cnt = edges_follows
        .join_map(&followers_ranked, |_follower, &followed, &()| followed)
        .distinct()
        .count_total()
        .map(|(pk, cnt)| (pk, TrustedPart::FollowerCnt(cnt)));

    let rank = normalized_ranks.map(|(pk, rank)| (pk, TrustedPart::Rank(rank)));

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
        .map(|(pk, ts)| (pk, TrustedPart::FirstCreatedAt(ts)));

    let post_cnt = events
        .filter(|e| e.is_root_post())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, post_cnt)| (pk, TrustedPart::PostCnt(post_cnt)));

    let reply_cnt = events
        .filter(|e| e.is_reply())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, reply_cnt)| (pk, TrustedPart::ReplyCnt(reply_cnt)));

    let reactions_cnt = events
        .filter(|e| e.kind == Kind::Reaction.as_u16())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, reactions_cnt)| (pk, TrustedPart::ReactionsCnt(reactions_cnt)));

    let zap_events = events
        .filter(|e| e.kind == Kind::ZapReceipt.as_u16())
        .flat_map(|e| {
            let day = e.created_at / 86_400;

            let mut sender: Option<Node> = None;
            let mut recipient: Option<Node> = None;
            let mut amount: Option<u64> = None;

            for tag in &e.tags {
                match tag {
                    EventTag::PubkeyUpper(pubkey) => {
                        if sender.is_none() {
                            sender = Some(*pubkey);
                        }
                    }
                    EventTag::Pubkey(pubkey) => {
                        if recipient.is_none() {
                            recipient = Some(*pubkey);
                        }
                    }
                    EventTag::Bolt11(bolt_amount) => {
                        amount = Some(*bolt_amount);
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

    let zap_sent_by_day = zap_events
        .flat_map(|(sender_opt, _recipient_opt, amount, day)| {
            sender_opt.map(|sender| ((sender, day), amount))
        })
        .reduce(|_key, vals, output| {
            let mut total_amt: u64 = 0;
            let mut total_cnt: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total_amt = total_amt.saturating_add(*amt * (*diff as u64));
                total_cnt += *diff;
            }

            if total_cnt > 0 {
                output.push(((total_amt, total_cnt), 1));
            }
        });

    let zap_sent_rollup = zap_sent_by_day
        .map(|((sender, _day), (amt, cnt))| (sender, (amt, cnt, 1u64)))
        .reduce(|_pk, vals, output| {
            let mut total_amt: u64 = 0;
            let mut total_cnt: Diff = 0;
            let mut days: u64 = 0;

            for &(&(amt, cnt, day_one), diff) in vals.iter() {
                if diff <= 0 {
                    continue;
                }

                total_amt = total_amt.saturating_add(amt.saturating_mul(diff as u64));
                total_cnt += cnt * diff;
                days = days.saturating_add(day_one.saturating_mul(diff as u64));
            }

            if days > 0 {
                output.push(((total_amt, total_cnt, days), 1));
            }
        });

    let zap_amt_sent =
        zap_sent_rollup.map(|(pk, (amt, _cnt, _days))| (pk, TrustedPart::ZapAmtSent(amt)));

    let zap_amt_recd = zap_events
        .flat_map(|(_sender, recipient_opt, amount, _day)| {
            recipient_opt.map(|recipient| (recipient, amount))
        })
        .reduce(|_pk, vals, output| {
            let mut total: u64 = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total = total.saturating_add(*amt * (*diff as u64));
            }

            if total != 0 {
                output.push((total, 1));
            }
        })
        .map(|(pk, zap_amt_recd)| (pk, TrustedPart::ZapAmtRecd(zap_amt_recd)));

    let zap_cnt_sent =
        zap_sent_rollup.map(|(pk, (_amt, cnt, _days))| (pk, TrustedPart::ZapCntSent(cnt)));

    let zap_cnt_recd = zap_events
        .flat_map(|(_sender, recipient, _amount, _day)| recipient)
        .count_total()
        .map(|(pk, zap_cnt_recd)| (pk, TrustedPart::ZapCntRecd(zap_cnt_recd)));

    let zap_avg_amt_day_sent = zap_sent_rollup.map(|(pk, (amt, _cnt, days))| {
        let avg = if days == 0 { 0 } else { amt / days };
        (pk, TrustedPart::ZapAvgAmtDaySent(avg))
    });

    let zap_recd_by_day = zap_events
        .flat_map(|(_sender_opt, recipient_opt, amount, day)| {
            recipient_opt.map(|recipient| ((recipient, day), amount))
        })
        .reduce(|_key, vals, output| {
            let mut total: u64 = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total = total.saturating_add(*amt * (*diff as u64));
            }

            if total != 0 {
                output.push((total, 1));
            }
        });

    let zap_avg_amt_day_recd = zap_recd_by_day
        .map(|((recipient, _day), amount)| (recipient, amount))
        .reduce(|_pk, vals, output| {
            let mut total: u64 = 0;
            let mut days: Diff = 0;

            for (amt, diff) in vals.iter() {
                if *diff <= 0 {
                    continue;
                }

                total = total.saturating_add(*amt * (*diff as u64));
                days += *diff;
            }

            if days > 0 {
                let avg = total / (days as u64);
                if avg != 0 {
                    output.push((avg, 1));
                }
            }
        })
        .map(|(pk, zap_avg_amt_day_recd)| {
            (pk, TrustedPart::ZapAvgAmtDayRecd(zap_avg_amt_day_recd))
        });

    let reports_cnt_sent = events
        .filter(|e| e.has_report())
        .map(|e| e.pubkey)
        .count_total()
        .map(|(pk, reports_cnt_sent)| (pk, TrustedPart::ReportsCntSent(reports_cnt_sent)));

    let reports_cnt_recd = events
        .flat_map(|e| e.reported_pubkeys())
        .count_total()
        .map(|(pk, reports_cnt_recd)| (pk, TrustedPart::ReportsCntRecd(reports_cnt_recd)));

    // let topic_counts = events
    //     .flat_map(|e| e.hashtags_for_author())
    //     .count_total()
    //     .map(|((pk, topic), count)| (pk, (topic, count)))
    //     .reduce(|_pk, vals, output| {
    //         let mut topics: Vec<(u32, Diff)> = Vec::new();
    //
    //         for &(&(topic, count), diff) in vals.iter() {
    //             if diff <= 0 {
    //                 continue;
    //             }
    //
    //             topics.push((topic, count));
    //         }
    //
    //         if !topics.is_empty() {
    //             output.push((topics, 1));
    //         }
    //     })
    //     .map(|(pk, topics)| (pk, TrustedPart::Topics(topics)));

    let active_hours = events
        .map(|e| {
            let hour = ((e.created_at / 3600) % 24) as u8;
            (e.pubkey, (hour, hour))
        })
        .reduce(|_pk, hours, output| {
            let mut min_h = u8::MAX;
            let mut max_h = 0u8;

            for &(&(h1, h2), diff) in hours.iter() {
                if diff > 0 {
                    min_h = min_h.min(h1);
                    max_h = max_h.max(h2);
                }
            }

            if min_h <= max_h {
                output.push(((min_h, max_h), 1 as Diff));
            }
        });

    let active_hours_start =
        active_hours.map(|(pk, (h, _))| (pk, TrustedPart::ActiveHoursStart(h)));

    let active_hours_end = active_hours.map(|(pk, (_, h))| (pk, TrustedPart::ActiveHoursEnd(h)));

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
        // .concat(&topic_counts)
        .concat(&active_hours_start)
        .concat(&active_hours_end);

    // Merge all parts into a single TrustedAssertion per pubkey
    parts.reduce(|_pk, vals, output| {
        let mut agg = TrustedAssertion::default();

        for (part, diff) in vals.iter() {
            if *diff <= 0 {
                continue;
            }

            match part {
                TrustedPart::FollowerCnt(v) => agg.follower_cnt += v * *diff,
                TrustedPart::Rank(v) => {
                    agg.rank = Some(*v);
                }
                TrustedPart::FirstCreatedAt(ts) => {
                    agg.first_created_at = match agg.first_created_at {
                        None => Some(*ts),
                        Some(cur) => Some(cur.min(*ts)),
                    };
                }
                TrustedPart::PostCnt(v) => agg.post_cnt += v * *diff,
                TrustedPart::ReplyCnt(v) => agg.reply_cnt += v * *diff,
                TrustedPart::ReactionsCnt(v) => agg.reactions_cnt += v * *diff,
                TrustedPart::ZapAmtRecd(v) => {
                    agg.zap_amt_recd = agg
                        .zap_amt_recd
                        .saturating_add(v.saturating_mul(*diff as u64));
                }
                TrustedPart::ZapAmtSent(v) => {
                    agg.zap_amt_sent = agg
                        .zap_amt_sent
                        .saturating_add(v.saturating_mul(*diff as u64));
                }
                TrustedPart::ZapCntRecd(v) => agg.zap_cnt_recd += v * *diff,
                TrustedPart::ZapCntSent(v) => agg.zap_cnt_sent += v * *diff,
                TrustedPart::ZapAvgAmtDayRecd(v) => {
                    agg.zap_avg_amt_day_recd = agg
                        .zap_avg_amt_day_recd
                        .saturating_add(v.saturating_mul(*diff as u64));
                }
                TrustedPart::ZapAvgAmtDaySent(v) => {
                    agg.zap_avg_amt_day_sent = agg
                        .zap_avg_amt_day_sent
                        .saturating_add(v.saturating_mul(*diff as u64));
                }
                TrustedPart::ReportsCntSent(v) => agg.reports_cnt_sent += v * *diff,
                TrustedPart::ReportsCntRecd(v) => agg.reports_cnt_recd += v * *diff,
                // TrustedPart::Topics(topics) => {
                //     agg.topics.extend(topics.iter().cloned());
                // }
                TrustedPart::ActiveHoursStart(h) => {
                    agg.active_hours_start = match agg.active_hours_start {
                        None => Some(*h),
                        Some(cur) => Some(cur.min(*h)),
                    };
                }
                TrustedPart::ActiveHoursEnd(h) => {
                    agg.active_hours_end = match agg.active_hours_end {
                        None => Some(*h),
                        Some(cur) => Some(cur.max(*h)),
                    };
                }
            }
        }

        output.push((agg, 1));
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::InputSession;
    use persist::edges::EdgeLabel;
    use persist::tag::EventTag;
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::execute_directly;

    use crate::algorithms::pagerank::pagerank;
    use crate::types::Diff;

    const PUBKEY1: Node = 1;
    const PUBKEY2: Node = 2;
    const PUBKEY3: Node = 3;
    const PUBKEY4: Node = 4;
    const PUBKEY5: Node = 5;
    const PUBKEY_LN: Node = 9;

    const HASHTAG_NOSTR: u32 = 400;
    const HASHTAG_WOT: u32 = 401;

    fn build_dataflow<FBuild>(build_inputs: FBuild) -> Vec<(Node, TrustedAssertion, u64, Diff)>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRecord, Diff>) + Send + Sync + 'static,
    {
        let (tx, rx) = channel();

        execute_directly(move |worker| {
            let mut events_input: InputSession<u64, EventRecord, Diff> = InputSession::new();
            let probe = ProbeHandle::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let events = events_input.to_collection(scope);
                let follows = events
                    .flat_map(|e| e.to_edges())
                    .filter(|&(kind, _, _, _)| kind == Kind::ContactList.as_u16())
                    .filter(|&(_, _, _, label)| label == EdgeLabel::Pubkey)
                    .map(|(_, from, to, _)| (from, to));

                let ranks = pagerank(20, &follows);

                let assertions = trusted_assertions(&events, &follows, &ranks);

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
            events_input.insert(EventRecord {
                id: 30,
                pubkey: PUBKEY1,
                created_at: 2 * 3600,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY2)],
            });
            events_input.insert(EventRecord {
                id: 31,
                pubkey: PUBKEY2,
                created_at: 10 * 3600,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY1)],
            });
            events_input.insert(EventRecord {
                id: 32,
                pubkey: PUBKEY3,
                created_at: 7 * 3600,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY1)],
            });

            // pubkey1 root posts
            events_input.insert(EventRecord {
                id: 10,
                pubkey: PUBKEY1,
                created_at: 3600,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Hashtag(HASHTAG_NOSTR)],
            });
            events_input.insert(EventRecord {
                id: 11,
                pubkey: PUBKEY1,
                created_at: 3 * 3600,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Hashtag(HASHTAG_WOT)],
            });

            // pubkey1 replies at hour 20
            events_input.insert(EventRecord {
                id: 12,
                pubkey: PUBKEY1,
                created_at: 20 * 3600,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Reply(9)],
            });

            // pubkey1 reacts to note 10 from pubkey1
            events_input.insert(EventRecord {
                id: 13,
                pubkey: PUBKEY1,
                created_at: 21 * 3600,
                kind: Kind::Reaction.as_u16(),
                tags: vec![EventTag::Mention(10), EventTag::Pubkey(PUBKEY1)],
            });

            // pubkey1 reacts to note 11 from pubkey1
            events_input.insert(EventRecord {
                id: 14,
                pubkey: PUBKEY1,
                created_at: 22 * 3600,
                kind: Kind::Reaction.as_u16(),
                tags: vec![EventTag::Mention(11), EventTag::Pubkey(PUBKEY1)],
            });

            // pubkey1 reports pubkey2
            events_input.insert(EventRecord {
                id: 15,
                pubkey: PUBKEY1,
                created_at: 23 * 3600,
                kind: Kind::Reporting.as_u16(),
                tags: vec![EventTag::PubkeyReport(PUBKEY2)],
            });

            events_input.insert(EventRecord {
                id: 16,
                pubkey: PUBKEY2,
                created_at: 5 * 3600,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY1)],
            });

            events_input.insert(EventRecord {
                id: 17,
                pubkey: PUBKEY3,
                created_at: 6 * 3600,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY1)],
            });

            events_input.insert(EventRecord {
                id: 18,
                pubkey: PUBKEY3,
                created_at: 7 * 3600,
                kind: Kind::TextNote.as_u16(),
                tags: vec![],
            });

            events_input.insert(EventRecord {
                id: 19,
                pubkey: PUBKEY3,
                created_at: 8 * 3600,
                kind: Kind::Reaction.as_u16(),
                tags: vec![],
            });

            // pubkey1 zaps pubkey2 1000 sats
            events_input.insert(EventRecord {
                id: 20,
                pubkey: PUBKEY_LN,
                created_at: 9 * 3600,
                kind: Kind::ZapReceipt.as_u16(),
                tags: vec![
                    EventTag::Pubkey(PUBKEY2),
                    EventTag::PubkeyUpper(PUBKEY1),
                    EventTag::Bolt11(1000),
                ],
            });
            // pubkey2 zaps pubkey1 2000 sats
            events_input.insert(EventRecord {
                id: 21,
                pubkey: PUBKEY_LN,
                created_at: 10 * 3600,
                kind: Kind::ZapReceipt.as_u16(),
                tags: vec![
                    EventTag::Pubkey(PUBKEY1),
                    EventTag::PubkeyUpper(PUBKEY2),
                    EventTag::Bolt11(2000),
                ],
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
            // let mut topics = res1.topics.clone();
            // topics.sort();
            // let mut expected_topics = vec![(HASHTAG_WOT, 1), (HASHTAG_NOSTR, 1)];
            // let mut expected_topics = vec![];
            // expected_topics.sort();

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
                // topics: expected_topics.clone(),
                active_hours_start: Some(1),
                active_hours_end: Some(23),
            };

            // let mut res1_sorted = res1.clone();
            // res1_sorted.topics = topics;

            // assert_eq!(expected_topics, res1_sorted.topics);
            // assert_eq!(expected, res1_sorted);
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
                // topics: vec![],
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
            events_input.insert(EventRecord {
                id: 1,
                pubkey: PUBKEY2,
                created_at: 2000,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY1)],
            });

            events_input.insert(EventRecord {
                id: 2,
                pubkey: PUBKEY1,
                created_at: 2000,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY2)],
            });

            events_input.insert(EventRecord {
                id: 3,
                pubkey: PUBKEY3,
                created_at: 2000,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY4)],
            });
            events_input.insert(EventRecord {
                id: 4,
                pubkey: PUBKEY4,
                created_at: 2000,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY5)],
            });
            events_input.flush();

            // pubkey1 unfollows pubkey2 at timestamp 2
            events_input.advance_to(2);
            events_input.remove(EventRecord {
                id: 2,
                pubkey: PUBKEY1,
                created_at: 2000,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY2)],
            });
            events_input.flush();

            // pubkey1 follows pubkey2 again at timestamp 3
            events_input.advance_to(3);

            events_input.insert(EventRecord {
                id: 4,
                pubkey: PUBKEY1,
                created_at: 4000,
                kind: Kind::ContactList.as_u16(),
                tags: vec![EventTag::Pubkey(PUBKEY2)],
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

    const PUBKEY_NO_EVENTS: Node = 7;

    #[test]
    fn assert_pubkey_without_events() {
        let captured = build_dataflow(|events_input| {
            events_input.insert(EventRecord {
                id: 42,
                pubkey: PUBKEY1,
                created_at: 1_000,
                kind: Kind::Reporting.as_u16(),
                tags: vec![EventTag::PubkeyReport(PUBKEY_NO_EVENTS)],
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
