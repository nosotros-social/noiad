use anyhow::{Context, Error, Result};
use differential_dataflow::{Hashable, VecCollection};
use futures::StreamExt;
use nostr_sdk::Keys;
use persist::event::EventRaw;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Exchange;
use timely::dataflow::{Scope, StreamCore};
use types::traits::IntoNostrEvent;
use types::types::Diff;

use crate::dataflow::DataflowConfig;
use crate::operators::builder_async::{AsyncOperatorBuilder, Event as AsyncEvent};

const BATCH_SIZE: usize = 20000;

pub fn event_sink<G, T>(
    scope: &G,
    config: DataflowConfig,
    input: &VecCollection<G, (String, T), Diff>,
) -> StreamCore<G, Vec<()>>
where
    G: Scope<Timestamp = u64>,
    T: IntoNostrEvent + timely::ExchangeData,
{
    let exchanged = input
        .inner
        .exchange(|((identifier, _payload), _time, _diff)| identifier.hashed());
    let mut builder = AsyncOperatorBuilder::new("EventSink".to_string(), scope.clone());
    let mut input = builder.new_disconnected_input(&exchanged, Pipeline);
    let worker_id = scope.index();
    let (_done_output, done_stream) = builder.new_output::<CapacityContainerBuilder<Vec<()>>>();

    let _ = builder.build_fallible(move |capability_sets| {
        Box::pin(async move {
            let done_caps = &mut capability_sets[0];
            let startup_checkpoint = config.persist.load_checkpoint()?.unwrap_or(0);
            let mut buffer: Vec<(String, T, u64, Diff)> = Vec::with_capacity(BATCH_SIZE);

            while let Some(event) = input.next().await {
                match event {
                    AsyncEvent::Data(_time, mut data) => {
                        for ((identifier, payload), ts, diff) in data.drain(..) {
                            buffer.push((identifier, payload, ts, diff));
                            if buffer.len() >= BATCH_SIZE {
                                flush_events(worker_id, &config, startup_checkpoint, &mut buffer)
                                    .await
                                    .context("event sink flush failed")?;
                            }
                        }
                    }
                    AsyncEvent::Progress(frontier) => {
                        if !buffer.is_empty() {
                            flush_events(worker_id, &config, startup_checkpoint, &mut buffer)
                                .await
                                .context("event sink flush failed")?;
                        }
                        done_caps.downgrade(frontier.iter().clone());
                        if frontier.is_empty() {
                            break;
                        }
                    }
                }
            }

            tracing::info!("[Worker {}] event sink completed", worker_id);
            done_caps.downgrade(std::iter::empty::<u64>());
            Ok::<(), Error>(())
        })
    });

    done_stream
}

async fn flush_events<T: IntoNostrEvent>(
    worker_id: usize,
    config: &DataflowConfig,
    startup_checkpoint: u64,
    buffer: &mut Vec<(String, T, u64, Diff)>,
) -> Result<()> {
    let nsec = config
        .trusted_assertions_nsec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("trusted_assertions_nsec not configured"))?;

    let keys = Keys::parse(nsec)?;
    let started = std::time::Instant::now();
    let buffer_len = buffer.len();
    let mut updates = Vec::with_capacity(buffer_len);

    for (id, payload, ts, diff) in buffer.drain(..) {
        if diff <= 0 {
            continue;
        }

        if !config.recompute && ts < startup_checkpoint {
            continue;
        }

        let event = payload.into_nostr_event(id, &keys)?;
        let event_raw = EventRaw {
            id: event.id.to_bytes(),
            pubkey: event.pubkey.to_bytes(),
            created_at: event.created_at.as_u64() as u32,
            content: event.content.into_bytes(),
            sig: event.sig.serialize(),
            kind: event.kind.as_u16(),
            tags_json: serde_json::to_vec(&event.tags).unwrap_or_default(),
        };

        let persisted_ts = if ts == 0 { 1 } else { ts };
        updates.push((event_raw, persisted_ts, diff));
    }

    config.persist.apply_generated_updates(&updates)?;

    if !updates.is_empty() {
        tracing::info!(
            "[Worker {}] event sink flushed={} in {:?}",
            worker_id,
            updates.len(),
            started.elapsed()
        );
    }
    updates.clear();

    if config.recompute {
        // Recompute can generate millions of replacement notifications. Yield after
        // each persisted batch so persist_source can drain the live notification
        // channel instead of letting event_sink build a large backlog.
        tokio::task::yield_now().await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use nostr_sdk::{Keys, Tag, TagKind};
    use persist::{event::EventRaw, helpers::TestStore};
    use types::{traits::IntoNostrEvent, trusted_assertions::ta_user::TrustedUser};

    #[test]
    fn trusted_assertion_into_nostr_event_tags_match() {
        let keys = Keys::generate();
        let identifier = keys.public_key().to_hex();

        let assertion = TrustedUser {
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

        let event = assertion
            .into_nostr_event(identifier.clone(), &keys)
            .unwrap();

        let expected_tags = nostr_sdk::Tags::from_list(vec![
            Tag::identifier(&identifier),
            Tag::public_key(identifier.parse().unwrap()),
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

    #[test]
    fn trusted_assertion_event_round_trips_through_persist() {
        let store = TestStore::default();
        let keys = Keys::generate();
        let identifier = keys.public_key().to_hex();

        let assertion = TrustedUser {
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

        let event = assertion.into_nostr_event(identifier, &keys).unwrap();
        let event_raw = EventRaw {
            id: event.id.to_bytes(),
            pubkey: event.pubkey.to_bytes(),
            created_at: event.created_at.as_u64() as u32,
            content: event.content.clone().into_bytes(),
            sig: event.sig.serialize(),
            kind: event.kind.as_u16(),
            tags_json: serde_json::to_vec(&event.tags).unwrap(),
        };

        let record = store.insert_event(&event_raw).unwrap().unwrap();
        let original = store.get_original_event(record.id).unwrap().unwrap();

        assert_eq!(original.id, event.id);
        assert_eq!(original.pubkey, event.pubkey);
        assert_eq!(original.kind, event.kind);
        assert_eq!(original.tags, event.tags);
        assert_eq!(original.content, event.content);
    }
}
