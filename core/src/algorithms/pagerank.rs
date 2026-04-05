use differential_dataflow::{
    AsCollection, ExchangeData, Hashable, VecCollection,
    lattice::Lattice,
    operators::{iterate::Variable, *},
};
use timely::{
    dataflow::{Scope, operators::core::Filter},
    order::Product,
};
use types::types::Diff;

use crate::types::Rank;

type Iter = usize;

/// Adapted from differential-dataflow examples to make it personalized from a collection of seeds.
/// For reference: https://github.com/TimelyDataflow/differential-dataflow/blob/master/differential-dataflow/examples/pagerank.rs
pub fn pagerank<G, D>(
    iters: Iter,
    edges: &VecCollection<G, (D, D), Diff>,
    seeds: &VecCollection<G, D, Diff>,
) -> VecCollection<G, D, Rank>
where
    G: Scope<Timestamp: Lattice>,
    D: ExchangeData + Hashable,
    (D, isize): Hashable,
{
    let seeds = seeds.distinct();
    let degrs = edges.map(|(src, _dst)| src).count();

    edges.scope().iterative::<Iter, _, _>(|inner| {
        let edges = edges.enter(inner);
        let degrs = degrs.enter(inner);
        let seeds = seeds.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = seeds.explode(|node| Some((node, 6_000_000)));
        let reset = seeds.explode(|node| Some((node, 1_000_000)));

        // Define a recursive variable to track surfers.
        // We start from `inits` and cycle only `iters`.
        let ranks = Variable::new_from(inits, Product::new(Default::default(), 1));

        // Match each surfer with the degree, scale numbers down.
        let to_push = degrs
            .semijoin(&ranks)
            .threshold(|(_node, degr), rank| (5 * rank) / (6 * degr))
            .map(|(node, _degr)| node);

        // Propagate surfers along links, blend in reset surfers.
        let mut pushed = edges
            .semijoin(&to_push)
            .map(|(_node, dest)| dest)
            .concat(&reset)
            .consolidate();

        if iters > 0 {
            pushed = pushed
                .inner
                .filter(move |(_d, t, _r)| t.inner < iters)
                .as_collection();
        }

        // Bind the recursive variable, return its limit.
        ranks.set(&pushed);
        pushed.leave()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::InputSession;
    use differential_dataflow::operators::CountTotal;
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::capture::{Capture, Extract};
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::execute_directly;

    fn build_pagerank_dataflow<FBuild>(
        iters: usize,
        build_inputs: FBuild,
    ) -> Vec<(u32, u64, isize, isize)>
    where
        FBuild: FnOnce(&mut InputSession<u64, (u32, u32), isize>, &mut InputSession<u64, u32, isize>)
            + Send
            + Sync
            + 'static,
    {
        let (tx, rx) = channel();

        execute_directly(move |worker| {
            let mut edges_input: InputSession<u64, (u32, u32), isize> = InputSession::new();
            let mut seeds_input: InputSession<u64, u32, isize> = InputSession::new();
            let probe = ProbeHandle::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let edges = edges_input.to_collection(scope);
                let seeds = seeds_input.to_collection(scope);
                let ranks = pagerank(iters, &edges, &seeds).count_total();

                ranks.probe_with(&probe).inner.capture_into(tx);
            });

            edges_input.advance_to(1);
            seeds_input.advance_to(1);
            build_inputs(&mut edges_input, &mut seeds_input);
            edges_input.flush();
            seeds_input.flush();

            let mut final_ts = (*edges_input.time()).max(*seeds_input.time());
            final_ts += 1;

            edges_input.advance_to(final_ts);
            seeds_input.advance_to(final_ts);
            edges_input.flush();
            seeds_input.flush();

            while probe.less_than(&final_ts) {
                worker.step();
            }
        });

        let mut ranks: Vec<(u32, u64, isize, isize)> = rx
            .extract()
            .into_iter()
            .flat_map(|(_cap_ts, batch)| {
                batch
                    .into_iter()
                    .map(|((node, rank), ts, diff)| (node, ts, rank, diff))
            })
            .collect();
        ranks.sort_by_key(|(_, ts, _, _)| *ts);
        ranks
    }

    fn get_ranks_at_time(ranks: &[(u32, u64, isize, isize)], time: u64) -> Vec<(u32, isize)> {
        ranks
            .iter()
            .filter(|(_, ts, _, diff)| *ts == time && *diff > 0)
            .map(|(n, _, r, _)| (*n, *r))
            .collect::<Vec<_>>()
    }

    #[test]
    fn assert_all_nodes_as_seeds_exact_regression() {
        let ranks = build_pagerank_dataflow(20, |edges_input, seeds_input| {
            for edge in [(1, 2), (2, 1), (3, 1), (4, 1), (5, 1)] {
                edges_input.insert(edge);
            }

            // pretty much global pagerank here
            for node in [1, 2, 3, 4, 5] {
                seeds_input.insert(node);
            }

            edges_input.flush();
            seeds_input.flush();

            edges_input.advance_to(2);
            seeds_input.advance_to(2);
            edges_input.remove((1, 2));
            edges_input.flush();
            seeds_input.flush();

            edges_input.advance_to(3);
            seeds_input.advance_to(3);
            edges_input.insert((1, 2));
        });

        let ranks_t1 = get_ranks_at_time(&ranks, 1);
        let ranks_t2 = get_ranks_at_time(&ranks, 2);
        let ranks_t3 = get_ranks_at_time(&ranks, 3);
        assert_eq!(ranks_t1.len(), 5);
        assert_eq!(ranks_t2.len(), 2);
        assert_eq!(ranks_t3.len(), 2);
        assert!(ranks_t1.iter().all(|(_, rank)| *rank > 0));
        assert!(ranks_t2.iter().all(|(_, rank)| *rank > 0));
        assert!(ranks_t3.iter().all(|(_, rank)| *rank > 0));
    }

    #[test]
    fn assert_seed_ranks() {
        let ranks = build_pagerank_dataflow(20, |edges_input, seeds_input| {
            for edge in [(1, 2), (2, 1), (2, 3), (3, 2), (4, 1), (5, 1)] {
                edges_input.insert(edge);
            }

            seeds_input.insert(1);
            seeds_input.insert(2);
        });

        let ranks_t1 = get_ranks_at_time(&ranks, 1);
        assert_eq!(ranks_t1.len(), 3);
        assert!(ranks_t1.iter().all(|(_, rank)| *rank > 0));
        let rank_of = |node: u32| {
            ranks_t1
                .iter()
                .find(|(n, _)| *n == node)
                .map(|(_, rank)| *rank)
                .unwrap_or(0)
        };
        assert!(rank_of(2) > rank_of(1));
        assert!(rank_of(1) > rank_of(3));
        assert_eq!(rank_of(4), 0);
        assert_eq!(rank_of(5), 0);
    }

    #[test]
    fn assert_sybil_ranks() {
        let ranks = build_pagerank_dataflow(20, |edges_input, seeds_input| {
            for edge in [
                (1, 2),
                (2, 1),
                (2, 3),
                (3, 2),
                (4, 1),
                (5, 1),
                (100, 101),
                (101, 100),
                (101, 102),
                (102, 101),
                (102, 103),
                (103, 102),
                (103, 100),
                (100, 103),
                (100, 1),
                (3, 100),
            ] {
                edges_input.insert(edge);
            }

            seeds_input.insert(1);
            seeds_input.insert(2);
        });

        let ranks_t1 = get_ranks_at_time(&ranks, 1);
        let rank_of = |node: u32| {
            ranks_t1
                .iter()
                .find(|(n, _)| *n == node)
                .map(|(_, rank)| *rank)
                .unwrap_or(0)
        };
        // the honest side keeps most of the rank.
        assert_eq!(rank_of(1), 3_161_228);
        assert_eq!(rank_of(2), 4_397_987);
        assert_eq!(rank_of(3), 1_832_715);

        // node 100 still gets some rank through node 3.
        assert_eq!(rank_of(100), 1_182_811);

        // the rest of the cluster gets less rank.
        assert_eq!(rank_of(101), 503_018);
        assert_eq!(rank_of(102), 419_180);
        assert_eq!(rank_of(103), 503_018);
    }

    #[test]
    fn assert_bridge_removal() {
        let ranks = build_pagerank_dataflow(20, |edges_input, seeds_input| {
            for edge in [
                (1, 2),
                (2, 1),
                (2, 3),
                (3, 2),
                (4, 1),
                (5, 1),
                (100, 101),
                (101, 100),
                (101, 102),
                (102, 101),
                (102, 103),
                (103, 102),
                (103, 100),
                (100, 103),
                (100, 1),
                (3, 100),
            ] {
                edges_input.insert(edge);
            }

            seeds_input.insert(1);
            seeds_input.insert(2);
            edges_input.flush();
            seeds_input.flush();

            edges_input.advance_to(2);
            seeds_input.advance_to(2);
            edges_input.remove((3, 100));
            edges_input.flush();
            seeds_input.flush();

            edges_input.advance_to(3);
            seeds_input.advance_to(3);
            edges_input.insert((3, 100));
        });

        let rank_of_at = |node: u32, time: u64| {
            get_ranks_at_time(&ranks, time)
                .iter()
                .find(|(n, _)| *n == node)
                .map(|(_, r)| *r)
                .unwrap_or(0)
        };

        let sybil_100_t1 = rank_of_at(100, 1);
        let sybil_100_t2 = rank_of_at(100, 2);
        let sybil_100_t3 = rank_of_at(100, 3);

        assert_eq!(sybil_100_t1, 1_182_811);

        // removing the bridge cuts node 100 off completely.
        assert_eq!(sybil_100_t2, 0);
        assert_eq!(sybil_100_t3, sybil_100_t1);
    }

    #[test]
    fn assert_seed_addition() {
        let ranks = build_pagerank_dataflow(20, |edges_input, seeds_input| {
            for edge in [
                (1, 2),
                (2, 1),
                (2, 3),
                (3, 2),
                (10, 11),
                (11, 10),
                (11, 12),
                (12, 11),
                (3, 10),
            ] {
                edges_input.insert(edge);
            }

            seeds_input.insert(1);
            edges_input.flush();
            seeds_input.flush();

            edges_input.advance_to(2);
            seeds_input.advance_to(2);
            seeds_input.insert(10);
        });

        let rank_of_at = |node: u32, time: u64| {
            get_ranks_at_time(&ranks, time)
                .iter()
                .find(|(n, _)| *n == node)
                .map(|(_, r)| *r)
                .unwrap_or(0)
        };

        let node_10_t1 = rank_of_at(10, 1);
        let node_10_t2 = rank_of_at(10, 2);
        assert_eq!(node_10_t1, 610_315);

        // adding node 10 as a seed pulls much more rank into its side.
        assert_eq!(node_10_t2, 2_782_248);
    }
}
