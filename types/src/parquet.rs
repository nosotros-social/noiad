use crate::{
    trusted_assertions::ta_user::TrustedUser,
    types::{Diff, Node},
};
use arrow_array::{ArrayRef, Int32Array, Int64Array, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub trait ToParquet {
    fn as_parquet(&self) -> (Arc<Schema>, Vec<ArrayRef>);
}

pub trait SnapshotSink: Default + ToParquet {
    type Item;

    fn apply_diff(&mut self, item: Self::Item, diff: Diff);

    fn rows(&self) -> usize;
}

#[derive(Clone, Debug, Default)]
pub struct FeaturesParquet(pub HashMap<Node, TrustedUser>);

impl ToParquet for FeaturesParquet {
    fn as_parquet(&self) -> (Arc<Schema>, Vec<ArrayRef>) {
        let mut rows: Vec<_> = self.0.iter().collect();
        rows.sort_unstable_by_key(|(node, _)| *node);

        let mut node_ids = Vec::with_capacity(rows.len());
        let mut follower_cnts = Vec::with_capacity(rows.len());
        let mut ranks = Vec::with_capacity(rows.len());
        let mut first_created_ats = Vec::with_capacity(rows.len());
        let mut active_hours_start = Vec::with_capacity(rows.len());
        let mut active_hours_end = Vec::with_capacity(rows.len());
        let mut reports_cnt_sent = Vec::with_capacity(rows.len());
        let mut reports_cnt_recd = Vec::with_capacity(rows.len());
        let mut post_cnt = Vec::with_capacity(rows.len());
        let mut reply_cnt = Vec::with_capacity(rows.len());
        let mut reactions_cnt = Vec::with_capacity(rows.len());
        let mut zap_amt_sent = Vec::with_capacity(rows.len());
        let mut zap_amt_recd = Vec::with_capacity(rows.len());
        let mut zap_cnt_sent = Vec::with_capacity(rows.len());
        let mut zap_cnt_recd = Vec::with_capacity(rows.len());
        let mut zap_avg_amt_day_sent = Vec::with_capacity(rows.len());
        let mut zap_avg_amt_day_recd = Vec::with_capacity(rows.len());

        for (node, assertion) in rows {
            node_ids.push(*node);
            follower_cnts.push(assertion.follower_cnt);
            ranks.push(assertion.rank.map(|rank| rank as i64));
            first_created_ats.push(assertion.first_created_at);
            active_hours_start.push(assertion.active_hours_start);
            active_hours_end.push(assertion.active_hours_end);
            reports_cnt_sent.push(assertion.reports_cnt_sent);
            reports_cnt_recd.push(assertion.reports_cnt_recd);
            post_cnt.push(assertion.post_cnt);
            reply_cnt.push(assertion.reply_cnt);
            reactions_cnt.push(assertion.reactions_cnt);
            zap_amt_sent.push(assertion.zap_amt_sent);
            zap_amt_recd.push(assertion.zap_amt_recd);
            zap_cnt_sent.push(assertion.zap_cnt_sent);
            zap_cnt_recd.push(assertion.zap_cnt_recd);
            zap_avg_amt_day_sent.push(assertion.zap_avg_amt_day_sent);
            zap_avg_amt_day_recd.push(assertion.zap_avg_amt_day_recd);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("node_id", DataType::UInt32, false),
            Field::new("follower_cnt", DataType::Int32, false),
            Field::new("rank", DataType::Int64, true),
            Field::new("first_created_at", DataType::UInt32, true),
            Field::new("active_hours_start", DataType::UInt8, true),
            Field::new("active_hours_end", DataType::UInt8, true),
            Field::new("reports_cnt_sent", DataType::Int32, false),
            Field::new("reports_cnt_recd", DataType::Int32, false),
            Field::new("post_cnt", DataType::Int32, false),
            Field::new("reply_cnt", DataType::Int32, false),
            Field::new("reactions_cnt", DataType::Int32, false),
            Field::new("zap_amt_sent", DataType::UInt64, false),
            Field::new("zap_amt_recd", DataType::UInt64, false),
            Field::new("zap_cnt_sent", DataType::Int32, false),
            Field::new("zap_cnt_recd", DataType::Int32, false),
            Field::new("zap_avg_amt_day_sent", DataType::UInt64, false),
            Field::new("zap_avg_amt_day_recd", DataType::UInt64, false),
        ]));

        let columns = vec![
            Arc::new(UInt32Array::from(node_ids)) as ArrayRef,
            Arc::new(Int32Array::from(follower_cnts)) as ArrayRef,
            Arc::new(Int64Array::from(ranks)) as ArrayRef,
            Arc::new(UInt32Array::from(first_created_ats)) as ArrayRef,
            Arc::new(UInt8Array::from(active_hours_start)) as ArrayRef,
            Arc::new(UInt8Array::from(active_hours_end)) as ArrayRef,
            Arc::new(Int32Array::from(reports_cnt_sent)) as ArrayRef,
            Arc::new(Int32Array::from(reports_cnt_recd)) as ArrayRef,
            Arc::new(Int32Array::from(post_cnt)) as ArrayRef,
            Arc::new(Int32Array::from(reply_cnt)) as ArrayRef,
            Arc::new(Int32Array::from(reactions_cnt)) as ArrayRef,
            Arc::new(UInt64Array::from(zap_amt_sent)) as ArrayRef,
            Arc::new(UInt64Array::from(zap_amt_recd)) as ArrayRef,
            Arc::new(Int32Array::from(zap_cnt_sent)) as ArrayRef,
            Arc::new(Int32Array::from(zap_cnt_recd)) as ArrayRef,
            Arc::new(UInt64Array::from(zap_avg_amt_day_sent)) as ArrayRef,
            Arc::new(UInt64Array::from(zap_avg_amt_day_recd)) as ArrayRef,
        ];

        (schema, columns)
    }
}

impl SnapshotSink for FeaturesParquet {
    type Item = (Node, TrustedUser);

    fn apply_diff(&mut self, item: Self::Item, diff: Diff) {
        let (node, assertion) = item;

        if diff > 0 {
            self.0.insert(node, assertion);
        } else if diff < 0 && self.0.get(&node) == Some(&assertion) {
            self.0.remove(&node);
        }
    }

    fn rows(&self) -> usize {
        self.0.len()
    }
}

#[derive(Clone, Debug, Default)]
pub struct EdgesParquet(pub HashSet<(Node, Node)>);

impl ToParquet for EdgesParquet {
    fn as_parquet(&self) -> (Arc<Schema>, Vec<ArrayRef>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::UInt32, false),
            Field::new("dst", DataType::UInt32, false),
        ]));

        let mut rows: Vec<_> = self.0.iter().copied().collect();
        rows.sort_unstable();

        let mut src = Vec::with_capacity(rows.len());
        let mut dst = Vec::with_capacity(rows.len());

        for (s, d) in rows {
            src.push(s);
            dst.push(d);
        }

        let columns = vec![
            Arc::new(UInt32Array::from(src)) as ArrayRef,
            Arc::new(UInt32Array::from(dst)) as ArrayRef,
        ];

        (schema, columns)
    }
}

impl SnapshotSink for EdgesParquet {
    type Item = (Node, Node);

    fn apply_diff(&mut self, item: Self::Item, diff: Diff) {
        if diff > 0 {
            self.0.insert(item);
        } else if diff < 0 {
            self.0.remove(&item);
        }
    }

    fn rows(&self) -> usize {
        self.0.len()
    }
}
