use nostr_database::nostr::{Event, EventId, Filter, types::Timestamp, util::BoxedFuture};
use nostr_database::{
    Backend, DatabaseError, DatabaseEventStatus, Events, NostrDatabase, SaveEventStatus,
};

pub use store::db::NostrSurrealDB;
use store::error::Error;
pub mod store;

impl NostrDatabase for NostrSurrealDB {
    fn backend(&self) -> Backend {
        Backend::Custom("SurrealDB".into())
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            self.insert(&event.to_owned())
                .await
                .map_err(|e| DatabaseError::backend(Error::Anyhow(e)))
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
                .map_err(|e| DatabaseError::backend(Error::Anyhow(e)))?
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
                .map_err(|e| DatabaseError::backend(Error::Anyhow(e)))
        })
    }

    fn count(&self, filter: Filter) -> BoxedFuture<Result<usize, DatabaseError>> {
        Box::pin(async move {
            self.count(filter)
                .await
                .map_err(|e| DatabaseError::backend(Error::Anyhow(e)))
        })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<Result<Events, DatabaseError>> {
        Box::pin(async move {
            self.query(filter)
                .await
                .map_err(|e| DatabaseError::backend(Error::Anyhow(e)))
        })
    }

    fn negentropy_items(
        &self,
        _filter: Filter,
    ) -> BoxedFuture<Result<Vec<(EventId, Timestamp)>, DatabaseError>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn delete(&self, _filter: Filter) -> BoxedFuture<Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }

    fn wipe(&self) -> BoxedFuture<Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }
}
