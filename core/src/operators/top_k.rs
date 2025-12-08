use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;
use differential_dataflow::{ExchangeData, VecCollection};
use timely::dataflow::Scope;

pub trait TopK<G, D>
where
    G: Scope,
    D: ExchangeData,
{
    fn top_k(self, k: usize) -> VecCollection<G, D, isize>;
}

impl<G, D> TopK<G, D> for VecCollection<G, D, isize>
where
    G: Scope,
    G::Timestamp: Lattice,
    D: ExchangeData,
{
    fn top_k(self, k: usize) -> VecCollection<G, D, isize> {
        self.map(|d| ((), d))
            .reduce(move |_key, inputs, output| {
                let mut remaining = k as isize;

                for &(value, weight) in inputs.iter().rev() {
                    if weight > 0 {
                        let mut emit = remaining;
                        if weight < remaining {
                            emit = weight;
                        }
                        output.push((value.clone(), emit));
                        remaining -= emit;
                    }
                    if remaining <= 0 {
                        break;
                    }
                }
            })
            .map(|(_key, value)| value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::TopK;
    use differential_dataflow::consolidation::consolidate;
    use differential_dataflow::input::InputSession;
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::capture::{Capture, Extract};

    #[test]
    fn assert_top_k_3() {
        let (tx, rx) = channel();

        timely::execute_directly(move |worker| {
            let mut input: InputSession<u64, (String, i32), isize> = InputSession::new();

            worker.dataflow::<u64, _, _>(|scope| {
                input
                    .to_collection(scope)
                    .map(|(user, score)| (score, user))
                    .top_k(3)
                    .inner
                    .capture_into(tx.clone());
            });

            input.insert(("Alice".to_string(), 50));
            input.insert(("Bob".to_string(), 20));
            input.insert(("Carol".to_string(), 15));
            input.insert(("Dave".to_string(), 30));
            input.insert(("Erin".to_string(), 25));

            input.advance_to(1);
            input.flush();
            drop(input);
            while worker.step() {}
        });

        let mut rows: Vec<((i32, String), isize)> = rx
            .extract()
            .into_iter()
            .flat_map(|(_t, batch)| batch.into_iter().map(|(v, _, d)| (v, d)))
            .collect();

        consolidate(&mut rows);

        let mut topk: Vec<(String, i32)> = rows
            .into_iter()
            .map(|((score, user), _)| (user, score))
            .collect();

        topk.sort_by_key(|(_user_score_name, user_score_value)| -user_score_value);

        assert_eq!(
            topk,
            vec![
                ("Alice".to_string(), 50),
                ("Dave".to_string(), 30),
                ("Erin".to_string(), 25),
            ]
        );
    }
}
