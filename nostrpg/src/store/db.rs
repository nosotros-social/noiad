use std::fmt;

use nostr_database::nostr::filter::MatchEventOptions;
use nostr_database::{Events, RejectedReason, SaveEventStatus};
use nostr_sdk::{Event, EventId, Filter, Tags};

use sqlx::PgPool;
use sqlx::types::Json;

use crate::store::model::EventRecord;

pub struct NostrPg {
    pub pool: PgPool,
    pub max_events_per_query: usize,
}

impl fmt::Debug for NostrPg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Postgres")
            .field("pool", &self.pool)
            .finish()
    }
}

impl NostrPg {
    pub async fn new(
        database_url: String,
        max_events_per_query: Option<usize>,
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(&database_url).await?;
        #[cfg(not(test))]
        {
            sqlx::migrate!("./migrations").run(&pool).await?;
        }
        Ok(Self {
            pool,
            max_events_per_query: max_events_per_query.unwrap_or(500),
        })
    }

    pub async fn find_event_by_id(&self, id: &EventId) -> sqlx::Result<Option<Event>> {
        let record = sqlx::query_as!(
            EventRecord,
            r#"
            SELECT
                id,
                pubkey,
                content,
                kind,
                created_at,
                tags AS "tags: Json<Tags>",
                sig
            FROM events
            WHERE id = $1
            "#,
            id.to_string()
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(record.and_then(|r| r.try_into().ok()))
    }

    pub async fn find_replaceable_event(
        &self,
        kind: i32,
        pubkey: &str,
    ) -> sqlx::Result<Option<EventRecord>> {
        sqlx::query_as!(
            EventRecord,
            r#"
                SELECT
                    id,
                    pubkey,
                    content,
                    kind,
                    created_at,
                    tags AS "tags: Json<Tags>",
                    sig
                FROM events
                WHERE kind = $1 AND pubkey = $2
                ORDER BY created_at DESC
                LIMIT 1
            "#,
            kind,
            pubkey
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn find_addressable_event(
        &self,
        kind: i16,
        pubkey: &str,
        d: &str,
    ) -> sqlx::Result<Option<EventRecord>> {
        sqlx::query_as::<_, EventRecord>(
            "
            SELECT * FROM events
            WHERE kind = $1
            AND pubkey = $2
            AND EXISTS (
                SELECT 1
                FROM jsonb_array_elements(tags) AS tag(element)
                WHERE element->>0 = 'd' AND element->>1 = $3
            )
            ORDER BY created_at DESC
            LIMIT 1
        ",
        )
        .bind(kind)
        .bind(pubkey)
        .bind(d)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn query_by_ids(&self, ids: Vec<&EventId>) -> sqlx::Result<Vec<EventRecord>> {
        let string_ids = ids.iter().map(|id| id.to_string()).collect::<Vec<String>>();

        sqlx::query_as!(
            EventRecord,
            r#"
                SELECT
                    id,
                    pubkey,
                    content,
                    kind,
                    created_at,
                    tags AS "tags: Json<Tags>",
                    sig
                FROM events
                WHERE id = ANY($1)
                ORDER BY created_at DESC
            "#,
            &string_ids
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn query_by_tags(
        &self,
        filter: Filter,
        limit: i64,
    ) -> sqlx::Result<Vec<EventRecord>> {
        let (keys, values): (Vec<String>, Vec<String>) = filter
            .generic_tags
            .iter()
            .flat_map(|(key, value_set)| {
                value_set
                    .iter()
                    .map(move |value| (key.to_string(), value.clone()))
            })
            .unzip();
        sqlx::query_as!(
            EventRecord,
            r#"
            SELECT
                id,
                pubkey,
                content,
                kind,
                created_at,
                tags AS "tags: Json<Tags>",
                sig
            FROM events
            WHERE EXISTS (
                SELECT 1
                FROM unnest($1::text[], $2::text[]) AS filter_tag(k, v)
                WHERE tags @> jsonb_build_array(jsonb_build_array(k, v))
            )
            ORDER BY created_at DESC
            LIMIT $3
        "#,
            &keys,
            &values,
            limit
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn count(&self, filter: Filter) -> sqlx::Result<usize> {
        Ok(self.query(filter).await?.len())
    }

    pub async fn query(&self, filter: Filter) -> sqlx::Result<Events> {
        if let (Some(since), Some(until)) = (filter.since, filter.until)
            && since > until
        {
            return Err(sqlx::Error::Protocol(
                "Invalid filter: since > until".into(),
            ));
        }

        let limit = filter.limit.unwrap_or(self.max_events_per_query) as i64;

        let response: Vec<EventRecord> = if let Some(ids) = &filter.ids {
            let ids: Vec<&EventId> = ids.iter().collect();
            self.query_by_ids(ids).await?
        } else if !filter.generic_tags.is_empty() {
            self.query_by_tags(filter.clone(), limit).await?
        } else {
            let kinds_opt: Option<Vec<i16>> = filter
                .kinds
                .as_ref()
                .filter(|v| !v.is_empty())
                .map(|v| v.iter().map(|k| k.as_u16() as i16).collect());
            let authors_opt: Option<Vec<String>> = filter
                .authors
                .as_ref()
                .filter(|v| !v.is_empty())
                .map(|v| v.iter().map(|a| a.to_string()).collect());

            let kinds_ref: Option<&[i16]> = kinds_opt.as_deref();
            let authors_ref: Option<&[String]> = authors_opt.as_deref();
            let until_i: Option<i64> = filter.until.map(|t| t.as_u64() as i64);
            let since_i: Option<i64> = filter.since.map(|t| t.as_u64() as i64);

            sqlx::query_as!(
                EventRecord,
                r#"
                    SELECT
                        id,
                        pubkey,
                        content,
                        kind,
                        created_at,
                        tags AS "tags: Json<Tags>",
                        sig
                    FROM events
                    WHERE ($1::int2[]  IS NULL OR kind = ANY($1))
                    AND ($2::text[]  IS NULL OR pubkey = ANY($2))
                    AND ($3::bigint  IS NULL OR created_at <= $3)
                    AND ($4::bigint  IS NULL OR created_at >= $4)
                    ORDER BY created_at DESC
                    LIMIT $5
                "#,
                kinds_ref,
                authors_ref,
                until_i,
                since_i,
                limit
            )
            .fetch_all(&self.pool)
            .await?
        };

        let mut events = Events::new(&filter);
        let opts = MatchEventOptions::new();
        response
            .into_iter()
            .filter_map(|r| r.try_into().ok())
            .filter(|e| filter.match_event(e, opts))
            .for_each(|e| {
                events.insert(e);
            });

        Ok(events)
    }

    pub async fn insert(&self, event: &Event) -> sqlx::Result<SaveEventStatus> {
        if event.kind.is_replaceable()
            && let Some(stored) = self
                .find_replaceable_event(event.kind.as_u16() as i32, &event.pubkey.to_string())
                .await?
        {
            if stored.created_at > event.created_at.as_u64() as i64 {
                return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
            }
            self.remove_event(&stored).await?;
        }

        if event.kind.is_addressable()
            && let Some(identifier) = event.tags.identifier()
            && let Some(stored) = self
                .find_addressable_event(
                    event.kind.as_u16() as i16,
                    &event.pubkey.to_string(),
                    identifier,
                )
                .await?
        {
            if stored.created_at > event.created_at.as_u64() as i64 {
                return Ok(SaveEventStatus::Rejected(RejectedReason::Replaced));
            }
            self.remove_event(&stored).await?;
        }

        let data: EventRecord = event.clone().into();
        let tags = Json(&data.tags);

        sqlx::query(
            "INSERT INTO events (id, pubkey, created_at, kind, tags, content, sig)
            VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&data.id)
        .bind(&data.pubkey)
        .bind(data.created_at)
        .bind(data.kind)
        .bind(tags)
        .bind(&data.content)
        .bind(&data.sig)
        .execute(&self.pool)
        .await?;

        Ok(SaveEventStatus::Success)
    }

    async fn remove_event(&self, event: &EventRecord) -> sqlx::Result<()> {
        sqlx::query("DELETE FROM events WHERE id = $1")
            .bind(&event.id)
            .execute(&self.pool)
            .await?;
        Ok(())
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
        db: NostrPg,
    }

    impl Deref for TestDB {
        type Target = NostrPg;

        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl TestDB {
        fn new(pool: PgPool) -> Self {
            Self {
                db: NostrPg {
                    pool,
                    max_events_per_query: 500,
                },
            }
        }

        async fn add_event<T: NostrSigner>(&self, signer: &T, builder: EventBuilder) -> Event {
            let event = builder.sign(signer).await.unwrap();
            self.db.insert(&event).await.unwrap();
            event
        }
    }

    #[sqlx::test]
    async fn test_insert_event(pool: PgPool) {
        let db = TestDB::new(pool);
        let key1 = Keys::generate();
        db.add_event(&key1, EventBuilder::new(Kind::TextNote, "1"))
            .await;
        db.add_event(&key1, EventBuilder::new(Kind::TextNote, "2"))
            .await;
        db.add_event(&key1, EventBuilder::new(Kind::TextNote, "3"))
            .await;

        assert_eq!(db.count(Filter::new()).await.unwrap(), 3);
    }

    #[sqlx::test]
    async fn test_insert_replaceable_event(pool: PgPool) {
        let db = TestDB::new(pool);
        let key1 = Keys::generate();

        let event1 = EventBuilder::new(Kind::ContactList, "")
            .custom_created_at(Timestamp::from_secs(1737970000));
        let event2 = EventBuilder::new(Kind::ContactList, "")
            .custom_created_at(Timestamp::from_secs(1737970001));

        db.add_event(&key1, event1).await;
        let expected = db.add_event(&key1, event2).await;

        let res = db.query(Filter::new()).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().id, expected.id);
    }

    #[sqlx::test]
    async fn test_insert_addressable_event(pool: PgPool) {
        let db = TestDB::new(pool);

        let key1 = Keys::generate();
        let event1 = EventBuilder::new(Kind::LongFormTextNote, "")
            .custom_created_at(Timestamp::from_secs(1737970000))
            .tags(Tags::from_list(vec![
                Tag::from_standardized(TagStandard::Identifier("123".into())),
                Tag::from_standardized(TagStandard::Title("Article Title".into())),
            ]));

        let event2 = EventBuilder::new(Kind::LongFormTextNote, "")
            .custom_created_at(Timestamp::from_secs(1737970001))
            .tags(Tags::from_list(vec![
                Tag::from_standardized(TagStandard::Identifier("123".into())),
                Tag::from_standardized(TagStandard::Title("Article Title".into())),
            ]));

        db.add_event(&key1, event1).await;

        let expected = db.add_event(&key1, event2).await;

        let res = db.query(Filter::new()).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().id, expected.id);
    }

    #[sqlx::test]
    async fn test_query_filters(pool: PgPool) {
        let db = TestDB::new(pool);

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
            .query(Filter::new().kind(Kind::TextNote).author(key1.public_key))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&event1));

        let res = db
            .query(Filter::new().kind(Kind::TextNote).author(key2.public_key))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&event2));

        let res = db
            .query(Filter::new().kind(Kind::TextNote).author(key3.public_key))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first(), Some(&event3));

        let res = db
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
            .query(
                Filter::new()
                    .kind(Kind::ContactList)
                    .author(key2.public_key),
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 0);

        let res = db
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

    #[sqlx::test]
    async fn test_query_tags(pool: PgPool) {
        let db = TestDB::new(pool);

        let key1 = Keys::generate();
        let key2 = Keys::generate();
        let key3 = Keys::generate();

        let build1 = EventBuilder::new(Kind::TextNote, "A")
            .tags(Tags::from_list(vec![Tag::public_key(key2.public_key())]));
        let build2 = EventBuilder::new(Kind::TextNote, "B")
            .tags(Tags::from_list(vec![Tag::public_key(key1.public_key())]));
        let build3 = EventBuilder::new(Kind::TextNote, "C")
            .tags(Tags::from_list(vec![Tag::public_key(key3.public_key())]));

        let event1 = db.add_event(&key1, build1).await;
        let event2 = db.add_event(&key2, build2).await;
        let event3 = db.add_event(&key3, build3).await;

        let res = db
            .query(
                Filter::new()
                    .kind(Kind::TextNote)
                    .custom_tag(
                        SingleLetterTag::lowercase(Alphabet::P),
                        key1.public_key.to_string(),
                    )
                    .custom_tag(
                        SingleLetterTag::lowercase(Alphabet::P),
                        key2.public_key.to_string(),
                    )
                    .limit(10),
            )
            .await
            .unwrap();

        assert_eq!(res.len(), 2);
        assert!(res.contains(&event1));
        assert!(res.contains(&event2));
        assert!(!res.contains(&event3));
    }
}
