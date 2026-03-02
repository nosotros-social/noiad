use std::hash::Hash;

use differential_dataflow::{
    ExchangeData, VecCollection,
    lattice::Lattice,
    operators::{Count, Iterate, Join, Threshold},
};
use timely::dataflow::Scope;
use types::types::Diff;

type K = isize;

pub fn k_core<G, D>(k: K, edges: &VecCollection<G, (D, D), Diff>) -> VecCollection<G, D, Diff>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    D: ExchangeData + Hash,
{
    let nodes = edges
        .flat_map(|(x, y)| Some(x).into_iter().chain(Some(y)))
        .distinct();

    nodes.iterate(|inner| {
        let scope = inner.scope();
        let edges = edges.enter(&scope);

        // get the edges in the current iteration.
        let filtered_edges = edges
            .semijoin(inner)
            .map(|(src, dst)| (dst, src))
            .semijoin(inner)
            .map(|(dst, src)| (src, dst));

        let degrees = filtered_edges.map(|(_, dst)| dst).count();

        degrees
            .filter(move |(_node, degree)| *degree >= k)
            .map(|(node, _degree)| node)
            .distinct()
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::mpsc::channel};

    use differential_dataflow::{consolidation::consolidate, input::InputSession};
    use timely::dataflow::operators::{Capture, capture::Extract};

    use crate::algorithms::k_core::k_core;

    #[test]
    fn assert_k_core_3() {
        let (tx, rx) = channel();

        timely::execute_directly(move |worker| {
            let mut input: InputSession<u64, (u64, u64), isize> = InputSession::new();

            worker.dataflow::<u64, _, _>(|scope| {
                let collection = input.to_collection(scope);

                k_core(3, &collection).inner.capture_into(tx.clone());
            });

            // 1:  [2, 3, 4, 5, 6]
            // 2:  [1, 3, 5, 6]
            // 3:  [1, 2, 5]
            // 4:  [1, 6]
            // 5:  [1, 2, 3]
            // 6:  [1, 2, 4]
            input.insert((1, 2));
            input.insert((1, 3));
            input.insert((1, 4));
            input.insert((1, 5));
            input.insert((1, 6));

            input.insert((2, 1));
            input.insert((2, 3));
            input.insert((2, 5));
            input.insert((2, 6));

            input.insert((3, 1));
            input.insert((3, 2));
            input.insert((3, 5));

            input.insert((4, 1));
            input.insert((4, 6));

            input.insert((5, 1));
            input.insert((5, 2));
            input.insert((5, 3));

            input.insert((6, 1));
            input.insert((6, 2));
            input.insert((6, 4));

            input.advance_to(1);
            input.close();
        });

        let mut node_diffs: Vec<(u64, isize)> = rx
            .extract()
            .into_iter()
            .flat_map(|(_t, batch)| batch.into_iter().map(|(value, _, diff)| (value, diff)))
            .collect();

        consolidate(&mut node_diffs);

        let final_nodes: HashSet<u64> = node_diffs
            .into_iter()
            .filter(|(_node, diff)| *diff > 0)
            .map(|(node, _)| node)
            .collect();

        let expected_nodes: HashSet<u64> = [1, 2, 3, 5].into_iter().collect();

        assert_eq!(final_nodes, expected_nodes);
    }
}
