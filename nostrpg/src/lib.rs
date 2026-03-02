use nostr_database::nostr::{Event, EventId, Filter, types::Timestamp, util::BoxedFuture};
use nostr_database::{
    Backend, DatabaseError, DatabaseEventStatus, Events, NostrDatabase, SaveEventStatus,
};
use std::time::Instant;

mod store;

pub use store::db::NostrPg;

impl NostrDatabase for NostrPg {
    fn backend(&self) -> Backend {
        Backend::Custom("Postgres".into())
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            let start = Instant::now();
            let res = self
                .insert(&event.to_owned())
                .await
                .map_err(DatabaseError::backend);
            tracing::info!(
                "New event {} kind {} created_at {} tags: {} in {:?}",
                &event.id.to_string()[0..10],
                &event.kind.to_string(),
                &event.created_at.to_human_datetime(),
                &event.tags.len(),
                start.elapsed()
            );
            res
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            match self
                .find_event_by_id(event_id)
                .await
                .map_err(DatabaseError::backend)?
            {
                Some(_) => Ok(DatabaseEventStatus::Saved),
                None => Ok(DatabaseEventStatus::NotExistent),
            }
        })
    }

    fn event_by_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<Option<Event>, DatabaseError>> {
        Box::pin(async move {
            self.find_event_by_id(event_id)
                .await
                .map_err(DatabaseError::backend)
        })
    }

    fn count(&self, filter: Filter) -> BoxedFuture<'_, Result<usize, DatabaseError>> {
        Box::pin(async move { self.count(filter).await.map_err(DatabaseError::backend) })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<'_, Result<Events, DatabaseError>> {
        Box::pin(async move { self.query(filter).await.map_err(DatabaseError::backend) })
    }

    fn negentropy_items(
        &self,
        _filter: Filter,
    ) -> BoxedFuture<'_, Result<Vec<(EventId, Timestamp)>, DatabaseError>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn delete(&self, _filter: Filter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }

    fn wipe(&self) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }
}
