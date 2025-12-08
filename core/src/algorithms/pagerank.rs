use differential_dataflow::{
    AsCollection, VecCollection,
    lattice::Lattice,
    operators::{iterate::Variable, *},
};
use timely::{
    dataflow::{Scope, operators::core::Filter},
    order::Product,
};

use crate::types::{Diff, Edge, Node};

type Iter = u32;

/// For reference: https://github.com/TimelyDataflow/differential-dataflow/blob/master/differential-dataflow/examples/pagerank.rs
pub fn pagerank<G>(
    iters: Iter,
    edges: &VecCollection<G, Edge, Diff>,
) -> VecCollection<G, Node, Diff>
where
    G: Scope<Timestamp: Lattice>,
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
