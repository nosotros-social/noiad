use anyhow::{Error, Result};
use differential_dataflow::VecCollection;
use futures::StreamExt;
use nostr_sdk::{Event, EventBuilder, EventId, Keys, Kind, PublicKey, Tag, TagKind};
use persist::db::PersistStore;
use persist::event::EventRaw;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, StreamCore};
use types::types::{Diff, Node};

use crate::algorithms::trusted_assertions::TrustedAssertion;
use crate::algorithms::trusted_assertions_event::EventAssertion;
use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event as AsyncEvent};
use crate::sinks::persist_sink;
use crate::types::IntoNostrEvent;

const BATCH_SIZE: usize = 20000;

impl IntoNostrEvent for TrustedAssertion {
    fn into_nostr_event(
        self,
        identifier: String,
        keys: &Keys,
        _persist_sink: &PersistStore,
    ) -> Result<Event> {
        let mut tags = vec![
            Tag::identifier(&identifier),
            Tag::custom(
                TagKind::Custom("follower_cnt".into()),
                [self.follower_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("post_cnt".into()),
                [self.post_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("reply_cnt".into()),
                [self.reply_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("reactions_cnt".into()),
                [self.reactions_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("zap_amt_recd".into()),
                [self.zap_amt_recd.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("zap_amt_sent".into()),
                [self.zap_amt_sent.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("zap_cnt_recd".into()),
                [self.zap_cnt_recd.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("zap_cnt_sent".into()),
                [self.zap_cnt_sent.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("reports_cnt_sent".into()),
                [self.reports_cnt_sent.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("reports_cnt_recd".into()),
                [self.reports_cnt_recd.to_string()],
            ),
        ];
        if let Some(rank) = self.rank {
            tags.push(Tag::custom(
                TagKind::Custom("rank".into()),
                [rank.to_string()],
            ));
        }
        if let Some(ts) = self.first_created_at {
            tags.push(Tag::custom(
                TagKind::Custom("first_created_at".into()),
                [ts.to_string()],
            ));
        }
        if let Some(h) = self.active_hours_start {
            tags.push(Tag::custom(
                TagKind::Custom("active_hours_start".into()),
                [h.to_string()],
            ));
        }
        if let Some(h) = self.active_hours_end {
            tags.push(Tag::custom(
                TagKind::Custom("active_hours_end".into()),
                [h.to_string()],
            ));
        }

        let event = EventBuilder::new(Kind::Custom(30382), "")
            .tags(tags)
            .sign_with_keys(keys)?;

        Ok(event)
    }
}

impl IntoNostrEvent for EventAssertion {
    fn into_nostr_event(self, id: String, keys: &Keys, _persist: &PersistStore) -> Result<Event> {
        let event_id = EventId::from_str(&id)?;

        let tags = vec![
            Tag::identifier(&id),
            Tag::event(event_id),
            Tag::custom(TagKind::Custom("rank".into()), [self.rank.to_string()]),
            Tag::custom(
                TagKind::Custom("comment_cnt".into()),
                [self.comment_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("quote_cnt".into()),
                [self.quote_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("repost_cnt".into()),
                [self.repost_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("reaction_cnt".into()),
                [self.reaction_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("zap_cnt".into()),
                [self.zap_cnt.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("zap_amount".into()),
                [self.zap_amount.to_string()],
            ),
        ];

        let event = EventBuilder::new(Kind::Custom(30383), "")
            .tags(tags)
            .sign_with_keys(keys)?;

        Ok(event)
    }
}

pub fn trusted_assertion_sink<G, T>(
    scope: &G,
    config: DataflowConfig,
    input: &VecCollection<G, (String, T), Diff>,
) -> StreamCore<G, Vec<()>>
where
    G: Scope<Timestamp = u64>,
    T: IntoNostrEvent + Clone + Send + 'static,
{
    let mut builder = AsyncOperatorBuilder::new("TrustedAssertionSink".to_string(), scope.clone());
    let mut input = builder.new_disconnected_input(&input.inner, Pipeline);
    let worker_id = scope.index();
    let (_done_output, done_stream) = builder.new_output::<CapacityContainerBuilder<Vec<()>>>();

    let _ = builder.build_fallible(move |capability_sets| {
        Box::pin(async move {
            let done_caps = &mut capability_sets[0];
            let mut buffer: Vec<(String, T, u64, Diff)> = Vec::with_capacity(BATCH_SIZE);

            while let Some(event) = input.next().await {
                match event {
                    AsyncEvent::Data(_time, mut data) => {
                        for ((node, assertion), ts, diff) in data.drain(..) {
                            buffer.push((node, assertion, ts, diff));
                            if buffer.len() >= BATCH_SIZE {
                                flush_assertions(worker_id, &config, &mut buffer).unwrap();
                            }
                        }
                    }
                    AsyncEvent::Progress(frontier) => {
                        if !buffer.is_empty() {
                            flush_assertions(worker_id, &config, &mut buffer).unwrap();
                        }
                        done_caps.downgrade(frontier.iter().clone());
                        if frontier.is_empty() {
                            break;
                        }
                    }
                }
            }

            tracing::info!("[Worker {}] Trusted assertion sink completed", worker_id);
            done_caps.downgrade(std::iter::empty::<u64>());
            Ok::<(), Error>(())
        })
    });

    done_stream
}

fn flush_assertions<T: IntoNostrEvent>(
    worker_id: usize,
    config: &DataflowConfig,
    buffer: &mut Vec<(String, T, u64, Diff)>,
) -> Result<()> {
    let nsec = config
        .trusted_assertions_nsec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("trusted_assertions_nsec not configured"))?;

    let keys = Keys::parse(nsec)?;
    let checkpoint = config.persist.load_checkpoint()?.unwrap_or(0);
    let started = std::time::Instant::now();
    let buffer_len = buffer.len();
    let mut updates = Vec::with_capacity(buffer_len);

    for (id, trusted_assertion, ts, diff) in buffer.drain(..) {
        if diff <= 0 {
            continue;
        }

        // Ignore events that were already processed
        if ts < checkpoint {
            continue;
        }

        let event = trusted_assertion.into_nostr_event(id, &keys, &config.persist)?;
        let event_raw = EventRaw {
            id: event.id.to_bytes(),
            pubkey: event.pubkey.to_bytes(),
            created_at: event.created_at.as_u64(),
            content: event.content.into_bytes(),
            sig: event.sig.serialize(),
            kind: event.kind.as_u16(),
            tags_json: serde_json::to_vec(&event.tags).unwrap_or_default(),
        };

        updates.push((event_raw, ts, diff));
    }

    config.persist.apply_updates(&updates)?;

    if updates.len() > 0 {
        tracing::info!(
            "[Worker {}] Trusted assertions sink flushed {} assertions in {:?}",
            worker_id,
            updates.len(),
            started.elapsed()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::{JsonUtil, Keys};
    use persist::helpers::TestStore;

    #[test]
    fn trusted_assertion_into_nostr_event_tags_match() {
        let keys = Keys::generate();
        let identifier = keys.public_key().to_hex();

        let assertion = TrustedAssertion {
            follower_cnt: 10,
            rank: Some(99),
            first_created_at: Some(1_700_000_000),
            post_cnt: 20,
            reply_cnt: 30,
            reactions_cnt: 40,
            zap_amt_recd: 50,
            zap_amt_sent: 60,
            zap_cnt_recd: 70,
            zap_cnt_sent: 80,
            zap_avg_amt_day_recd: 0,
            zap_avg_amt_day_sent: 0,
            reports_cnt_sent: 90,
            reports_cnt_recd: 100,
            active_hours_start: Some(9),
            active_hours_end: Some(17),
        };

        let persist = TestStore::default();
        let event = assertion
            .into_nostr_event(identifier.clone(), &keys, &persist)
            .unwrap();

        let expected_tags = nostr_sdk::Tags::from_list(vec![
            Tag::identifier(&identifier),
            Tag::custom(TagKind::Custom("follower_cnt".into()), [10.to_string()]),
            Tag::custom(TagKind::Custom("post_cnt".into()), [20.to_string()]),
            Tag::custom(TagKind::Custom("reply_cnt".into()), [30.to_string()]),
            Tag::custom(TagKind::Custom("reactions_cnt".into()), [40.to_string()]),
            Tag::custom(TagKind::Custom("zap_amt_recd".into()), [50.to_string()]),
            Tag::custom(TagKind::Custom("zap_amt_sent".into()), [60.to_string()]),
            Tag::custom(TagKind::Custom("zap_cnt_recd".into()), [70.to_string()]),
            Tag::custom(TagKind::Custom("zap_cnt_sent".into()), [80.to_string()]),
            Tag::custom(TagKind::Custom("reports_cnt_sent".into()), [90.to_string()]),
            Tag::custom(
                TagKind::Custom("reports_cnt_recd".into()),
                [100.to_string()],
            ),
            Tag::custom(TagKind::Custom("rank".into()), [99.to_string()]),
            Tag::custom(
                TagKind::Custom("first_created_at".into()),
                [1_700_000_000u64.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("active_hours_start".into()),
                [9u8.to_string()],
            ),
            Tag::custom(
                TagKind::Custom("active_hours_end".into()),
                [17u8.to_string()],
            ),
        ]);

        assert!(event.verify().is_ok());
        assert_eq!(event.tags, expected_tags);
    }
}
