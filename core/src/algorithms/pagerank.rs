use differential_dataflow::{
    AsCollection, ExchangeData, Hashable, VecCollection,
    lattice::Lattice,
    operators::{iterate::Variable, *},
};
use timely::{
    dataflow::{Scope, operators::core::Filter},
    order::Product,
};

use crate::types::Diff;

type Iter = usize;

/// For reference: https://github.com/TimelyDataflow/differential-dataflow/blob/master/differential-dataflow/examples/pagerank.rs
pub fn pagerank<G, D>(
    iters: Iter,
    edges: &VecCollection<G, (D, D), Diff>,
) -> VecCollection<G, D, Diff>
where
    G: Scope<Timestamp: Lattice>,
    D: ExchangeData + Hashable,
    (D, isize): Hashable,
{
    // initialize many surfers at each node.
    let nodes = edges
        .flat_map(|(x, y)| Some(x).into_iter().chain(Some(y)))
        .distinct();

    // snag out-degrees for each node.
    let degrs = edges.map(|(src, _dst)| src).count();

    edges.scope().iterative::<Iter, _, _>(|inner| {
        // Bring various collections into the scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);
        let degrs = degrs.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = nodes.explode(|node| Some((node, 6_000_000)));
        let reset = nodes.explode(|node| Some((node, 1_000_000)));

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
    use timely::execute_directly;

    #[test]
    fn assert_pagerank() {
        let (tx, rx) = channel();

        execute_directly(move |worker| {
            let mut edges_input: InputSession<u64, (u32, u32), isize> = InputSession::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let edges = edges_input.to_collection(scope);
                let ranks = pagerank(20, &edges).count_total();

                ranks.inner.capture_into(tx);
            });

            edges_input.advance_to(1);
            let graph_edges = vec![(1, 2), (2, 1), (3, 1), (4, 1), (5, 1)];

            for edge in graph_edges {
                edges_input.insert(edge);
            }

            edges_input.flush();

            edges_input.advance_to(2);
            // node1 unfollows node2
            edges_input.remove((1, 2));
            edges_input.flush();

            edges_input.advance_to(3);
            // node1 follows node2 again
            edges_input.insert((1, 2));
            edges_input.flush();
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

        let get_ranks_at_time = |time: u64| {
            ranks
                .iter()
                .filter(|(_, ts, _, diff)| *ts == time && *diff > 0)
                .map(|(n, _, r, _)| (*n, *r))
                .collect::<Vec<_>>()
        };

        let ranks_t1 = get_ranks_at_time(1);
        let ranks_t2 = get_ranks_at_time(2);
        let ranks_t3 = get_ranks_at_time(3);

        assert_eq!(
            ranks_t1,
            vec![
                (1, 13968397),
                (2, 13031592),
                (3, 1000000),
                (4, 1000000),
                (5, 1000000),
            ]
        );
        // Remove edge (1,2) causes rank of 2 to drop.
        // We also want to assure that node 3,4,5 ranks remain unchanged.
        assert_eq!(ranks_t2, vec![(1, 4333332), (2, 1000000)]);
        assert_eq!(ranks_t3, vec![(1, 13968397), (2, 13031592)]);
    }
}
