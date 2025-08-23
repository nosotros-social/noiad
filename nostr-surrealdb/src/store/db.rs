use anyhow::{Result, anyhow};
use nostr_database::nostr::event::{Event, EventId, Kind};
use nostr_database::nostr::filter::{Filter, MatchEventOptions};
use nostr_database::nostr::key::PublicKey;
use nostr_database::nostr::nips::nip01::Coordinate;
use nostr_database::{Events, RejectedReason, SaveEventStatus};
use std::fmt;
use std::sync::LazyLock;
use std::time::Instant;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::{RecordId, Surreal};

use super::error::Error::InvalidFilter;
use super::model::EventRecord;

pub struct NostrSurrealDB {
    db: LazyLock<Surreal<Any>>,
}

impl fmt::Debug for NostrSurrealDB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NostrSurrealDB")
            .field("db", &self.db)
            .finish()
    }
}

impl NostrSurrealDB {
    pub fn new() -> Self {
        Self {
            db: LazyLock::new(Surreal::init),
        }
    }

    pub async fn open(self, endpoint: &str) -> Result<Self> {
        self.db.connect(endpoint).with_capacity(0).await?;
        if !endpoint.starts_with("mem://") {
            self.db
                .signin(Root {
                    username: "root",
                    password: "root",
                })
                .await?;
        }

        self.db.use_ns("nostr").use_db("nostrsdb").await?;
        self.init().await?;
        Ok(self)
    }

    async fn init(&self) -> Result<()> {
        let query = "
            DEFINE TABLE IF NOT EXISTS event SCHEMAFULL;
            DEFINE FIELD IF NOT EXISTS id on event type string READONLY;
            DEFINE FIELD IF NOT EXISTS kind on event type int READONLY;
            DEFINE FIELD IF NOT EXISTS pubkey on event type string READONLY;
            DEFINE FIELD IF NOT EXISTS content on event type string READONLY;
            DEFINE FIELD IF NOT EXISTS created_at on event type int READONLY;
            DEFINE FIELD IF NOT EXISTS tags on event type array<array<string>> READONLY;
            DEFINE FIELD IF NOT EXISTS sig on event type string READONLY;

            DEFINE INDEX IF NOT EXISTS idx_event_id ON event COLUMNS id UNIQUE;
            DEFINE INDEX IF NOT EXISTS idx_event_kind ON event COLUMNS kind CONCURRENTLY;
            DEFINE INDEX IF NOT EXISTS idx_event_pubkey ON event COLUMNS pubkey CONCURRENTLY;
            DEFINE INDEX IF NOT EXISTS idx_event_created_at ON event COLUMNS created_at CONCURRENTLY;
        ";
        self.db.query(query).await?;
        Ok(())
    }

    pub async fn find_event_by_id(&self, id: &EventId) -> Result<Option<Event>> {
        let res: Option<EventRecord> = self.db.select(("event", id.to_string())).await?;
        res.map(Event::try_from).transpose()
    }

    async fn find_replaceable_event(
        &self,
        kind: Kind,
        pubkey: &PublicKey,
    ) -> Result<Option<EventRecord>> {
        let res: Option<EventRecord> = self
            .db
            .query("SELECT * FROM event WHERE kind=$kind AND pubkey=$pubkey")
            .bind(("kind", kind.as_u16()))
            .bind(("pubkey", pubkey.to_string()))
            .await?
            .take(0)?;
        Ok(res)
    }

    async fn find_addressable_event(&self, addr: &Coordinate) -> Result<Option<EventRecord>> {
        if !addr.kind.is_addressable() {
            return Ok(None);
        }

        let query = r#"
            SELECT * FROM event
            WHERE
                kind=$kind AND
                pubkey=$pubkey AND
                tags CONTAINS ["d", $d]
        "#;
        let res: Option<EventRecord> = self
            .db
            .query(query)
            .bind(("kind", addr.kind.as_u16()))
            .bind(("pubkey", addr.public_key.to_string()))
            .bind(("d", addr.identifier.to_string()))
            .await?
            .take(0)?;
        Ok(res)
    }

    pub async fn count(&self, filter: Filter) -> Result<usize> {
        Ok(self.query(filter).await?.len())
    }

    pub async fn query(&self, filter: Filter) -> Result<Events> {
        if let (Some(since), Some(until)) = (filter.since, filter.until) {
            if since > until {
                return Err(anyhow!(InvalidFilter));
            }
        }

        let limit = filter.limit.unwrap_or(500);
        let mut events = Events::new(&filter);
        let response: Vec<EventRecord>;

        if let Some(ids) = &filter.ids
            && !ids.is_empty()
        {
            let ids: Vec<RecordId> = ids
                .iter()
                .map(|id| RecordId::from_table_key("event", id.to_string()))
                .collect();

            response = self
                .db
                .query("SELECT * FROM event WHERE id IN $ids")
                .bind(("ids", ids))
                .await?
                .take(0)?;
        } else {
            let bind_kinds = filter
                .kinds
                .iter()
                .flatten()
                .map(|kind| kind.as_u16())
                .collect::<Vec<u16>>();

            let bind_authors = filter
                .authors
                .iter()
                .flatten()
                .map(|a| a.to_string())
                .collect::<Vec<String>>();

            let query = r#"
                SELECT * FROM event
                WHERE
                    IF $kinds { kind in $kinds } ELSE { true } AND
                    IF $authors { pubkey in $authors } ELSE { true } AND
                    IF $until { created_at <= $until } ELSE { true } AND
                    IF $since { created_at >= $since } ELSE { true }
                LIMIT $limit
            "#;
            response = self
                .db
                .query(query)
                .bind(("kinds", bind_kinds))
                .bind(("authors", bind_authors))
                .bind(("until", filter.until))
                .bind(("limit", limit))
                .await?
                .take(0)?;
        }

        let match_options = MatchEventOptions::new();
        response
            .iter()
            .filter_map(|event| event.clone().try_into().ok())
            .filter(|event| filter.match_event(event, match_options))
            .for_each(|event| {
                events.insert(event);
            });

        Ok(events)
    }

    pub async fn insert(&self, event: &Event) -> Result<SaveEventStatus> {
        let start = Instant::now();
        if event.kind.is_ephemeral() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Ephemeral));
        }

        if self.find_event_by_id(&event.id).await?.is_some() {
            return Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate));
        }

        if event.kind.is_replaceable() {
            if let Some(stored) = self
                .find_replaceable_event(event.kind, &event.pubkey)
                .await?
            {
                if stored.created_at > event.created_at {
                    return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                }
                self.remove_event(&stored).await?;
            }
        }

        if event.kind.is_addressable() {
            if let Some(identifier) = event.tags.identifier() {
                let coordinate: Coordinate =
                    Coordinate::new(event.kind, event.pubkey).identifier(identifier);

                if let Some(stored) = self.find_addressable_event(&coordinate).await? {
                    if stored.created_at > event.created_at {
                        return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
                    }

                    self.remove_event(&stored).await?;
                }
            }
        }

        let data: EventRecord = event.clone().into();
        let _: Option<EventRecord> = self.db.create("event").content(data).await?;

        dbg!(start.elapsed());
        dbg!(&event.id);
        Ok(SaveEventStatus::Success)
    }

    async fn remove_event(&self, event: &EventRecord) -> Result<Option<EventRecord>> {
        let deleted_event: Option<EventRecord> = self
            .db
            .delete(("event", event.id.key().to_string()))
            .await?;
        Ok(deleted_event)
    }
}

impl Default for NostrSurrealDB {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nostr_database::nostr::event::{EventBuilder, Kind};
    use nostr_database::nostr::event::{Tag, TagStandard, Tags};
    use nostr_database::nostr::filter::{Alphabet, SingleLetterTag};
    use nostr_database::nostr::signer::NostrSigner;
    use nostr_database::nostr::{key::Keys, types::Timestamp};
    use std::ops::Deref;

    struct TestDB {
        db: NostrSurrealDB,
    }

    impl Deref for TestDB {
        type Target = NostrSurrealDB;

        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl TestDB {
        fn new() -> Self {
            Self {
                db: NostrSurrealDB::new(),
            }
        }

        pub async fn open(mut self) -> Self {
            self.db = self.db.open("mem://").await.unwrap();
            self
        }

        async fn add_event<T: NostrSigner>(&self, signer: &T, builder: EventBuilder) -> Event {
            let event = builder
                .sign(signer)
                .await
                .map_err(|error| anyhow!(error))
                .unwrap();
            self.db.insert(&event).await.unwrap();
            event
        }
    }

    #[tokio::test]
    async fn test_insert_event() {
        let db = TestDB::new().open().await;

        let key1 = Keys::generate();
        db.add_event(&key1, EventBuilder::new(Kind::TextNote, "1"))
            .await;
        db.add_event(&key1, EventBuilder::new(Kind::TextNote, "2"))
            .await;
        db.add_event(&key1, EventBuilder::new(Kind::TextNote, "3"))
            .await;

        assert_eq!(db.count(Filter::new()).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_insert_replaceable_event() {
        let db = TestDB::new().open().await;
        let key1 = Keys::generate();

        let event1 = EventBuilder::new(Kind::ContactList, "")
            .custom_created_at(Timestamp::from_secs(1737970000));
        let event2 = EventBuilder::new(Kind::ContactList, "")
            .custom_created_at(Timestamp::from_secs(1737970001)); // replaced

        db.add_event(&key1, event1).await;
        let expected = db.add_event(&key1, event2).await;

        let res = db.query(Filter::new()).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().id, expected.id);
    }

    #[tokio::test]
    async fn test_insert_addressable_event() {
        let db = TestDB::new().open().await;

        let key1 = Keys::generate();
        let event1 = EventBuilder::new(Kind::LongFormTextNote, "")
            .custom_created_at(Timestamp::from_secs(1737970000))
            .tags(Tags::from_list(vec![Tag::from_standardized(
                TagStandard::Identifier("123".into()),
            )]));

        let event2 = EventBuilder::new(Kind::LongFormTextNote, "")
            .custom_created_at(Timestamp::from_secs(1737970001))
            .tags(Tags::from_list(vec![Tag::from_standardized(
                TagStandard::Identifier("123".into()),
            )]));

        db.add_event(&key1, event1).await;
        let expected = db.add_event(&key1, event2).await;

        let res = db.query(Filter::new()).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().id, expected.id);
    }

    #[tokio::test]
    async fn test_query_filters() {
        let db = TestDB::new().open().await;

        let key1 = Keys::generate();
        let key2 = Keys::generate();
        let key3 = Keys::generate();
        let build1 = EventBuilder::new(Kind::TextNote, "A");
        let build2 = EventBuilder::new(Kind::TextNote, "B");
        let build3 = EventBuilder::new(Kind::TextNote, "C");
        let build4 = EventBuilder::new(Kind::ContactList, "");
        let build5 = EventBuilder::new(Kind::RelayList, "");

        let event1 = db.add_event(&key1, build1).await;
        let event2 = db.add_event(&key2, build2).await;
        let event3 = db.add_event(&key3, build3).await;

        let event4 = db.add_event(&key1, build4).await;
        let event5 = db.add_event(&key2, build5).await;

        let res = db
            .db
            .query(Filter::new().kind(Kind::TextNote).author(key1.public_key))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&event1));

        let res = db
            .db
            .query(Filter::new().kind(Kind::TextNote).author(key2.public_key))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&event2));

        let res = db
            .db
            .query(Filter::new().kind(Kind::TextNote).author(key3.public_key))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&event3));

        let res = db
            .db
            .query(
                Filter::new()
                    .kinds([Kind::TextNote, Kind::ContactList])
                    .author(key1.public_key),
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 2);
        assert!(res.contains(&event1));
        assert!(res.contains(&event4));

        let res = db
            .db
            .query(
                Filter::new()
                    .kind(Kind::ContactList)
                    .author(key2.public_key),
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 0);

        let res = db
            .db
            .query(
                Filter::new()
                    .kinds([Kind::TextNote, Kind::ContactList, Kind::RelayList])
                    .authors([key1.public_key, key2.public_key]),
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 4);
        assert!(res.contains(&event1));
        assert!(res.contains(&event2));
        assert!(res.contains(&event4));
        assert!(res.contains(&event5));
    }

    #[tokio::test]
    async fn test_query_tags() {
        let db = TestDB::new().open().await;

        let key1 = Keys::generate();
        let key2 = Keys::generate();

        let build1 = EventBuilder::new(Kind::TextNote, "A")
            .tags(Tags::from_list(vec![Tag::public_key(key2.public_key)]));

        let build2 = EventBuilder::new(Kind::TextNote, "B")
            .tags(Tags::from_list(vec![Tag::public_key(key1.public_key)]));

        let event1 = db.add_event(&key1, build1).await;
        let event2 = db.add_event(&key2, build2).await;

        let res = db
            .db
            .query(Filter::new().kind(Kind::TextNote).custom_tag(
                SingleLetterTag::lowercase(Alphabet::P),
                key1.public_key.to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        assert!(res.contains(&event2));

        let res = db
            .db
            .query(Filter::new().kind(Kind::TextNote).custom_tag(
                SingleLetterTag::lowercase(Alphabet::P),
                key2.public_key.to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(res.len(), 1);
        assert!(res.contains(&event1));
    }

    #[tokio::test]
    async fn test_query_pagination() {
        let db = TestDB::new().open().await;

        let key1 = Keys::generate();

        let build1 = EventBuilder::new(Kind::TextNote, "A")
            .custom_created_at(Timestamp::from_secs(1737970000));

        let build2 = EventBuilder::new(Kind::TextNote, "B")
            .custom_created_at(Timestamp::from_secs(1737970010));

        let build3 = EventBuilder::new(Kind::TextNote, "C")
            .custom_created_at(Timestamp::from_secs(1737970020));

        let build4 = EventBuilder::new(Kind::TextNote, "C")
            .custom_created_at(Timestamp::from_secs(1737970040));

        let event1 = db.add_event(&key1, build1).await;
        let event2 = db.add_event(&key1, build2).await;
        let event3 = db.add_event(&key1, build3).await;
        let event4 = db.add_event(&key1, build4).await;

        let res = db
            .db
            .query(
                Filter::new()
                    .kind(Kind::TextNote)
                    .since(Timestamp::from_secs(1737970020)),
            )
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert!(res.contains(&event3));
        assert!(res.contains(&event4));

        let res = db
            .db
            .query(
                Filter::new()
                    .kind(Kind::TextNote)
                    .until(Timestamp::from_secs(1737970020)),
            )
            .await
            .unwrap();

        assert_eq!(res.len(), 3);
        assert!(res.contains(&event1));
        assert!(res.contains(&event2));
        assert!(res.contains(&event3));
    }
}
