use anyhow::Result;
use nostr_sdk::{Event, EventBuilder, Keys, Kind, Tag, TagKind};
use serde::{Deserialize, Serialize};

use crate::{traits::IntoNostrEvent, types::Diff};

pub type Count = i32;

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TrustedUser {
    pub first_created_at: Option<u32>,
    pub active_hours_start: Option<u8>,
    pub active_hours_end: Option<u8>,
    pub follower_cnt: Count,
    pub rank: Option<Diff>,
    pub reports_cnt_sent: Count,
    pub reports_cnt_recd: Count,
    pub post_cnt: Count,
    pub reply_cnt: Count,
    pub reactions_cnt: Count,
    pub zap_amt_sent: u64,
    pub zap_amt_recd: u64,
    pub zap_cnt_sent: Count,
    pub zap_cnt_recd: Count,
    pub zap_avg_amt_day_sent: u64,
    pub zap_avg_amt_day_recd: u64,
}

impl TrustedUser {
    pub fn merge(&mut self, other: &TrustedUser) {
        self.first_created_at = match (self.first_created_at, other.first_created_at) {
            (None, None) => None,
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (Some(a), Some(b)) => Some(a.min(b)),
        };
        self.active_hours_start = match (self.active_hours_start, other.active_hours_start) {
            (None, None) => None,
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (Some(a), Some(b)) => Some(a.min(b)),
        };
        self.active_hours_end = match (self.active_hours_end, other.active_hours_end) {
            (None, None) => None,
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (Some(a), Some(b)) => Some(a.max(b)),
        };
        self.follower_cnt += other.follower_cnt;
        self.rank = other.rank.or(self.rank);
        self.reports_cnt_sent += other.reports_cnt_sent;
        self.reports_cnt_recd += other.reports_cnt_recd;
        self.post_cnt += other.post_cnt;
        self.reply_cnt += other.reply_cnt;
        self.reactions_cnt += other.reactions_cnt;
        self.zap_amt_sent += other.zap_amt_sent;
        self.zap_amt_recd += other.zap_amt_recd;
        self.zap_cnt_sent += other.zap_cnt_sent;
        self.zap_cnt_recd += other.zap_cnt_recd;
        self.zap_avg_amt_day_sent = self.zap_avg_amt_day_sent.max(other.zap_avg_amt_day_sent);
        self.zap_avg_amt_day_recd = self.zap_avg_amt_day_recd.max(other.zap_avg_amt_day_recd);
    }

    pub fn is_empty(&self) -> bool {
        self.first_created_at.is_none()
            && self.active_hours_start.is_none()
            && self.active_hours_end.is_none()
            && self.follower_cnt == 0
            && self.rank.is_none()
            && self.reports_cnt_sent == 0
            && self.reports_cnt_recd == 0
            && self.post_cnt == 0
            && self.reply_cnt == 0
            && self.reactions_cnt == 0
            && self.zap_amt_sent == 0
            && self.zap_amt_recd == 0
            && self.zap_cnt_sent == 0
            && self.zap_cnt_recd == 0
            && self.zap_avg_amt_day_sent == 0
            && self.zap_avg_amt_day_recd == 0
    }

    pub fn first_created_at(&mut self, created_at: u32, diff: Diff) {
        // Append-only assumption: we only move the minimum on positive diffs.
        if diff > 0 {
            self.first_created_at = Some(match self.first_created_at {
                None => created_at,
                Some(current) => current.min(created_at),
            });
        }
    }

    pub fn active_hours(&mut self, hour: u8, diff: Diff) {
        if diff > 0 {
            self.active_hours_start = Some(match self.active_hours_start {
                None => hour,
                Some(current) => current.min(hour),
            });
            self.active_hours_end = Some(match self.active_hours_end {
                None => hour,
                Some(current) => current.max(hour),
            });
        }
    }

    pub fn apply_count_partial(count: &mut Count, partial: Count, diff: Diff) {
        if diff > 0 {
            *count += partial * diff as Count;
        } else {
            *count -= partial * (-diff) as Count;
        }
    }

    pub fn apply(&mut self, partial: &TrustedUser, diff: Diff) {
        Self::apply_count_partial(&mut self.follower_cnt, partial.follower_cnt, diff);
        Self::apply_count_partial(&mut self.reports_cnt_sent, partial.reports_cnt_sent, diff);
        Self::apply_count_partial(&mut self.reports_cnt_recd, partial.reports_cnt_recd, diff);
        Self::apply_count_partial(&mut self.post_cnt, partial.post_cnt, diff);
        Self::apply_count_partial(&mut self.reply_cnt, partial.reply_cnt, diff);
        Self::apply_count_partial(&mut self.reactions_cnt, partial.reactions_cnt, diff);
        Self::apply_count_partial(&mut self.zap_cnt_sent, partial.zap_cnt_sent, diff);
        Self::apply_count_partial(&mut self.zap_cnt_recd, partial.zap_cnt_recd, diff);

        if diff > 0 {
            self.zap_amt_sent = self.zap_amt_sent.saturating_add(partial.zap_amt_sent);
            self.zap_amt_recd = self.zap_amt_recd.saturating_add(partial.zap_amt_recd);
        } else {
            self.zap_amt_sent = self.zap_amt_sent.saturating_sub(partial.zap_amt_sent);
            self.zap_amt_recd = self.zap_amt_recd.saturating_sub(partial.zap_amt_recd);
        }

        if diff > 0 {
            if let Some(ts) = partial.first_created_at {
                self.first_created_at(ts, diff);
            }
            if let Some(hour) = partial.active_hours_start {
                self.active_hours(hour, diff);
            }
            if let Some(hour) = partial.active_hours_end {
                self.active_hours(hour, diff);
            }
            if let Some(rank) = partial.rank {
                self.rank = Some(rank);
            }
        }
    }
}

fn push_nonzero_tag<T>(tags: &mut Vec<Tag>, name: &str, value: T)
where
    T: PartialEq + Default + ToString,
{
    if value != T::default() {
        tags.push(Tag::custom(
            TagKind::Custom(name.into()),
            [value.to_string()],
        ));
    }
}

impl IntoNostrEvent for TrustedUser {
    fn into_nostr_event(self, identifier: String, keys: &Keys) -> Result<Event> {
        let mut tags = vec![Tag::identifier(&identifier)];
        push_nonzero_tag(&mut tags, "follower_cnt", self.follower_cnt);
        push_nonzero_tag(&mut tags, "post_cnt", self.post_cnt);
        push_nonzero_tag(&mut tags, "reply_cnt", self.reply_cnt);
        push_nonzero_tag(&mut tags, "reactions_cnt", self.reactions_cnt);
        push_nonzero_tag(&mut tags, "zap_amt_recd", self.zap_amt_recd);
        push_nonzero_tag(&mut tags, "zap_amt_sent", self.zap_amt_sent);
        push_nonzero_tag(&mut tags, "zap_cnt_recd", self.zap_cnt_recd);
        push_nonzero_tag(&mut tags, "zap_cnt_sent", self.zap_cnt_sent);
        push_nonzero_tag(&mut tags, "reports_cnt_sent", self.reports_cnt_sent);
        push_nonzero_tag(&mut tags, "reports_cnt_recd", self.reports_cnt_recd);

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
