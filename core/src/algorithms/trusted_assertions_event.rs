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
use types::event::Node;

use crate::types::Diff;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum EventPart {
    CommentCnt(Diff),
    QuoteCnt(Diff),
    RepostCnt(Diff),
    ReactionCnt(Diff),
    ZapCnt(Diff),
    ZapAmount(u64),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventAssertion {
    pub rank: u8,
    pub comment_cnt: Diff,
    pub quote_cnt: Diff,
    pub repost_cnt: Diff,
    pub reaction_cnt: Diff,
    pub zap_cnt: Diff,
    pub zap_amount: u64,
}

impl EventAssertion {
    fn compute_rank(&mut self) {
        let comment_score = (self.comment_cnt.max(0) as f64) * 3.0;
        let quote_score = (self.quote_cnt.max(0) as f64) * 4.0;
        let repost_score = (self.repost_cnt.max(0) as f64) * 2.0;
        let reaction_score = (self.reaction_cnt.max(0) as f64) * 1.0;

        let zap_score = if self.zap_cnt > 0 && self.zap_amount > 0 {
            let base = (self.zap_cnt.max(0) as f64) * 5.0;
            let amount_bonus = (self.zap_amount as f64).log10() * 2.0;
            base + amount_bonus
        } else {
            0.0
        };

        let raw_score = comment_score + quote_score + repost_score + reaction_score + zap_score;

        let normalized = if raw_score > 0.0 {
            (20.0 * (raw_score + 1.0).log10()).min(100.0)
        } else {
            0.0
        };

        self.rank = normalized.round() as u8;
    }
}

pub fn event_assertions_event<G>(
    events: &VecCollection<G, EventRecord, Diff>,
) -> VecCollection<G, (Node, EventAssertion), Diff>
where
    G: Scope,
    G::Timestamp: Lattice + TotalOrder,
{
    let comment_cnt = events
        .filter(|e| e.kind == Kind::TextNote.as_u16() && e.is_reply())
        .flat_map(|e| {
            e.tags
                .iter()
                .filter_map(|tag| match tag {
                    EventTag::Reply(id) => Some(*id),
                    EventTag::RootReply(id) => Some(*id),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .count_total()
        .map(|(id, cnt)| (id, EventPart::CommentCnt(cnt)));

    let quote_cnt = events
        .filter(|e| e.kind == Kind::TextNote.as_u16())
        .flat_map(|e| {
            e.tags
                .iter()
                .filter_map(|tag| match tag {
                    EventTag::Quote(id) => Some(*id),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .count_total()
        .map(|(id, cnt)| (id, EventPart::QuoteCnt(cnt)));

    let repost_cnt = events
        .filter(|e| e.kind == Kind::Repost.as_u16())
        .flat_map(|e| {
            e.tags
                .iter()
                .filter_map(|tag| match tag {
                    EventTag::Mention(id) => Some(*id),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .count_total()
        .map(|(id, cnt)| (id, EventPart::RepostCnt(cnt)));

    let reaction_cnt = events
        .filter(|e| e.kind == Kind::Reaction.as_u16())
        .flat_map(|e| {
            e.tags
                .iter()
                .filter_map(|tag| match tag {
                    EventTag::Mention(id) => Some(*id),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .count_total()
        .map(|(id, cnt)| (id, EventPart::ReactionCnt(cnt)));

    let zap_stats = events
        .filter(|e| e.kind == Kind::ZapReceipt.as_u16())
        .flat_map(|e| {
            let mut target_event: Option<Node> = None;
            let mut amount: u64 = 0;

            for tag in &e.tags {
                match tag {
                    EventTag::Mention(id) => {
                        if target_event.is_none() {
                            target_event = Some(*id);
                        }
                    }
                    EventTag::Bolt11(amt) => {
                        amount = *amt;
                    }
                    _ => {}
                }
            }

            target_event.map(|id| (id, amount))
        })
        .reduce(|_event_id, vals, output| {
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
                output.push(((total_cnt, total_amt), 1));
            }
        });

    let zap_cnt = zap_stats.map(|(id, (cnt, _amt))| (id, EventPart::ZapCnt(cnt)));
    let zap_amount = zap_stats.map(|(id, (_cnt, amt))| (id, EventPart::ZapAmount(amt)));

    let parts = comment_cnt
        .concat(&quote_cnt)
        .concat(&repost_cnt)
        .concat(&reaction_cnt)
        .concat(&zap_cnt)
        .concat(&zap_amount);

    parts.reduce(|_event_id, vals, output| {
        let mut assertion = EventAssertion::default();

        for (part, diff) in vals.iter() {
            if *diff <= 0 {
                continue;
            }

            match part {
                EventPart::CommentCnt(v) => assertion.comment_cnt += v * diff,
                EventPart::QuoteCnt(v) => assertion.quote_cnt += v * diff,
                EventPart::RepostCnt(v) => assertion.repost_cnt += v * diff,
                EventPart::ReactionCnt(v) => assertion.reaction_cnt += v * diff,
                EventPart::ZapCnt(v) => assertion.zap_cnt += v * diff,
                EventPart::ZapAmount(v) => {
                    assertion.zap_amount = assertion.zap_amount.saturating_add(v * (*diff as u64))
                }
            }
        }

        assertion.compute_rank();

        output.push((assertion, 1));
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::InputSession;
    use persist::tag::EventTag;
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::execute_directly;

    use crate::types::Diff;

    const PUBKEY1: u32 = 1;
    const PUBKEY2: u32 = 2;
    const PUBKEY3: u32 = 3;
    const PUBKEY_LN: u32 = 9;

    const EVENT_POST1: Node = 100;
    const EVENT_POST2: Node = 101;
    const EVENT_POST3: Node = 102;

    fn build_dataflow<FBuild>(build_inputs: FBuild) -> Vec<(Node, EventAssertion, u64, Diff)>
    where
        FBuild: FnOnce(&mut InputSession<u64, EventRecord, Diff>) + Send + Sync + 'static,
    {
        let (tx, rx) = channel();

        execute_directly(move |worker| {
            let mut events_input: InputSession<u64, EventRecord, Diff> = InputSession::new();
            let probe = ProbeHandle::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let events = events_input.to_collection(scope);
                let assertions = event_assertions_event(&events);
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
                    .map(|((event_id, res), ts, diff)| (event_id, res, ts, diff))
            })
            .collect()
    }

    fn get_captured_by_event(
        captured: &[(Node, EventAssertion, u64, Diff)],
        event_id: Node,
    ) -> Vec<(Node, EventAssertion, u64, Diff)> {
        let mut res = captured
            .iter()
            .filter(|(id, _, _, diff)| *id == event_id && *diff > 0)
            .cloned()
            .collect::<Vec<_>>();
        res.sort_by_key(|(_, _, ts, _)| *ts);
        res
    }

    #[test]
    fn assert_event_assertions() {
        let captured = build_dataflow(|events_input| {
            events_input.insert(EventRecord {
                id: EVENT_POST1,
                pubkey: PUBKEY1,
                created_at: 1000,
                kind: Kind::TextNote.as_u16(),
                tags: vec![],
            });
            events_input.insert(EventRecord {
                id: EVENT_POST2,
                pubkey: PUBKEY2,
                created_at: 1001,
                kind: Kind::TextNote.as_u16(),
                tags: vec![],
            });
            events_input.insert(EventRecord {
                id: EVENT_POST3,
                pubkey: PUBKEY3,
                created_at: 1002,
                kind: Kind::TextNote.as_u16(),
                tags: vec![],
            });

            // EVENT_POST1: 3 reactions, 2 comments, 1 quote, 1 repost, 2 zaps (8000 sats)
            events_input.insert(EventRecord {
                id: 200,
                pubkey: PUBKEY2,
                created_at: 2000,
                kind: Kind::Reaction.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 201,
                pubkey: PUBKEY3,
                created_at: 2001,
                kind: Kind::Reaction.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 202,
                pubkey: PUBKEY1,
                created_at: 2002,
                kind: Kind::Reaction.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 203,
                pubkey: PUBKEY2,
                created_at: 2003,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Reply(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 204,
                pubkey: PUBKEY3,
                created_at: 2004,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::RootReply(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 205,
                pubkey: PUBKEY2,
                created_at: 2005,
                kind: Kind::TextNote.as_u16(),
                tags: vec![EventTag::Quote(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 206,
                pubkey: PUBKEY3,
                created_at: 2006,
                kind: Kind::Repost.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST1)],
            });
            events_input.insert(EventRecord {
                id: 207,
                pubkey: PUBKEY_LN,
                created_at: 2007,
                kind: Kind::ZapReceipt.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST1), EventTag::Bolt11(5000)],
            });
            events_input.insert(EventRecord {
                id: 208,
                pubkey: PUBKEY_LN,
                created_at: 2008,
                kind: Kind::ZapReceipt.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST1), EventTag::Bolt11(3000)],
            });

            // EVENT_POST2: 1 reaction only
            events_input.insert(EventRecord {
                id: 300,
                pubkey: PUBKEY1,
                created_at: 3000,
                kind: Kind::Reaction.as_u16(),
                tags: vec![EventTag::Mention(EVENT_POST2)],
            });
        });

        let rows1 = get_captured_by_event(&captured, EVENT_POST1);
        assert_eq!(rows1.len(), 1);
        let assertion1 = &rows1[0].1;
        assert_eq!(assertion1.reaction_cnt, 3);
        assert_eq!(assertion1.comment_cnt, 2);
        assert_eq!(assertion1.quote_cnt, 1);
        assert_eq!(assertion1.repost_cnt, 1);
        assert_eq!(assertion1.zap_cnt, 2);
        assert_eq!(assertion1.zap_amount, 8000);
        assert_eq!(assertion1.rank, 31);

        let rows2 = get_captured_by_event(&captured, EVENT_POST2);
        println!("Captured EVENT_POST1 rows: {:?}", rows2);
        assert_eq!(rows2.len(), 1);
        let assertion2 = &rows2[0].1;
        assert_eq!(assertion2.rank, 6);
        assert_eq!(assertion2.reaction_cnt, 1);
        assert_eq!(assertion2.comment_cnt, 0);
        assert_eq!(assertion2.quote_cnt, 0);
        assert_eq!(assertion2.repost_cnt, 0);
        assert_eq!(assertion2.zap_cnt, 0);
        assert_eq!(assertion2.zap_amount, 0);

        // EVENT_POST3: no engagement
        let rows3 = get_captured_by_event(&captured, EVENT_POST3);
        assert_eq!(rows3.len(), 0);
    }

    #[test]
    fn assert_compute_event_rank() {
        let mut assertion = EventAssertion::default();
        assertion.compute_rank();
        assert_eq!(assertion.rank, 0);

        // 10 reactions = 10 * 1 = 10 score
        // rank = 20 * log10(11) = 21
        let mut assertion = EventAssertion {
            reaction_cnt: 10,
            ..Default::default()
        };
        assertion.compute_rank();
        assert_eq!(assertion.rank, 21);

        // 5 zaps of 10000 sats total
        // zap_score = 5 * 5 + log10(10000) * 2 = 25 + 8 = 33
        // rank = 20 * log10(34) = 31
        let mut assertion = EventAssertion {
            zap_cnt: 5,
            zap_amount: 10000,
            ..Default::default()
        };
        assertion.compute_rank();
        assert_eq!(assertion.rank, 31);

        // Viral event: 100 comments, 50 quotes, 200 reposts, 1000 reactions, 50 zaps, 1M sats
        // comment: 100 * 3 = 300
        // quote: 50 * 4 = 200
        // repost: 200 * 2 = 400
        // reaction: 1000 * 1 = 1000
        // zap: 50 * 5 + log10(1000000) * 2 = 250 + 12 = 262
        // total = 2162
        // rank = 20 * log10(2163) = 67
        let mut assertion = EventAssertion {
            comment_cnt: 100,
            quote_cnt: 50,
            repost_cnt: 200,
            reaction_cnt: 1000,
            zap_cnt: 50,
            zap_amount: 1_000_000,
            ..Default::default()
        };
        assertion.compute_rank();
        assert_eq!(assertion.rank, 67);

        // Super viral
        let mut assertion = EventAssertion {
            comment_cnt: 10000,
            quote_cnt: 10000,
            repost_cnt: 10000,
            reaction_cnt: 100000,
            zap_cnt: 10000,
            zap_amount: 1_000_000_000,
            ..Default::default()
        };
        assertion.compute_rank();
        assert!(assertion.rank <= 100);
    }
}
