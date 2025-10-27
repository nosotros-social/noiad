use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use timely::communication::Push;
use timely::container::{CapacityContainerBuilder, ContainerBuilder};
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputWrapper;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as TimelyOperatorBuilder;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::progress::Timestamp;
use timely::scheduling::SyncActivator;
use timely::{Data, container};

type InputBuf<T, D> = Rc<RefCell<VecDeque<(T, D)>>>;

pub struct AsyncOperatorBuilder<G: Scope> {
    builder: TimelyOperatorBuilder<G>,
    input_readers: Vec<Box<dyn FnMut()>>,
    waker_state: Arc<TimelyWaker>,
}

impl<G: Scope> AsyncOperatorBuilder<G> {
    pub fn new(name: String, scope: G) -> Self {
        let builder = TimelyOperatorBuilder::new(name, scope.clone());
        let info = builder.operator_info();
        let waker_state = Arc::new(TimelyWaker {
            activator: scope.sync_activator_for(info.address.to_vec()),
            active: AtomicBool::new(false),
            task_ready: AtomicBool::new(true),
        });

        Self {
            builder,
            input_readers: Vec::new(),
            waker_state,
        }
    }

    pub fn new_input<D: Data>(&mut self, stream: &Stream<G, D>) -> AsyncInput<G::Timestamp, D> {
        let mut input = self.builder.new_input(stream, Pipeline);
        let buf: InputBuf<G::Timestamp, D> = Rc::new(RefCell::new(VecDeque::new()));
        let buf_for_reader = buf.clone();

        let waker_cell = Rc::new(Cell::new(None::<Waker>));
        let waker_for_reader = waker_cell.clone();

        self.input_readers.push(Box::new(move || {
            input.for_each(|time, data| {
                let t = time.time().clone();
                for d in data.iter() {
                    buf_for_reader
                        .borrow_mut()
                        .push_back((t.clone(), d.clone()));
                }
            });
            if let Some(w) = waker_for_reader.take() {
                w.wake();
            }
        }));

        AsyncInput {
            buf,
            waker: waker_cell,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn new_output<CB: ContainerBuilder>(
        &mut self,
    ) -> (
        AsyncOutput<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
        StreamCore<G, CB::Container>,
    ) {
        let (wrapper, stream) = self.builder.new_output();
        let handler = AsyncOutput {
            wrapper: Rc::new(RefCell::new(wrapper)),
        };
        (handler, stream)
    }

    pub fn build<F, Fut>(mut self, logic: F)
    where
        F: FnOnce(Vec<Capability<G::Timestamp>>) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let waker_state = Arc::clone(&self.waker_state);
        let operator_waker = self.waker_state;
        let mut input_readers = std::mem::take(&mut self.input_readers);

        self.builder.build_reschedule(move |caps| {
            let mut fut = Some(Box::pin(logic(caps.clone())));
            let mut finished = false;
            let waker_state = Arc::clone(&waker_state);

            move |_frontiers| {
                operator_waker.active.store(true, Ordering::SeqCst);
                for r in input_readers.iter_mut() {
                    r();
                }
                operator_waker.active.store(false, Ordering::SeqCst);

                if !finished && waker_state.task_ready.swap(false, Ordering::SeqCst) {
                    let waker = futures_util::task::waker_ref(&waker_state);
                    let mut cx = Context::from_waker(&waker);
                    if let Some(f) = fut.as_mut()
                        && Pin::new(f).poll(&mut cx).is_ready()
                    {
                        finished = true;
                        fut = None;
                    }
                }

                !finished
            }
        });
    }

    pub fn build_fallible<F, E: 'static>(mut self, logic: F) -> StreamCore<G, Vec<Rc<E>>>
    where
        F: for<'a> FnOnce(
                &'a mut [CapabilitySet<G::Timestamp>],
            ) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'a>>
            + 'static,
    {
        let (error_handler, error_stream) =
            self.new_output::<CapacityContainerBuilder<Vec<Rc<E>>>>();

        self.build(move |mut caps| async move {
            let error_cap = caps.pop().unwrap();
            let mut caps = caps
                .into_iter()
                .map(CapabilitySet::from_elem)
                .collect::<Vec<_>>();
            if let Err(e) = logic(&mut caps).await {
                error_handler.give(&error_cap, Rc::new(e));
            }
        });

        error_stream
    }
}

pub struct AsyncInput<T, D> {
    buf: InputBuf<T, D>,
    waker: Rc<Cell<Option<Waker>>>,
}

impl<T, D> AsyncInput<T, D> {
    pub async fn next(&mut self) -> Option<(T, D)> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<(T, D)>> {
        if let Some(item) = self.buf.borrow_mut().pop_front() {
            return Poll::Ready(Some(item));
        }
        self.waker.set(Some(cx.waker().clone()));
        Poll::Pending
    }
}

pub struct AsyncOutput<T, CB, P>
where
    T: Timestamp,
    CB: ContainerBuilder,
    P: Push<Message<T, CB::Container>> + 'static,
{
    wrapper: Rc<RefCell<OutputWrapper<T, CB, P>>>,
}

impl<T, C, P> AsyncOutput<T, CapacityContainerBuilder<C>, P>
where
    T: Timestamp,
    C: timely::Container + Clone + 'static,
    P: Push<Message<T, C>>,
{
    pub fn give<D>(&self, cap: &Capability<T>, data: D)
    where
        CapacityContainerBuilder<C>: container::PushInto<D>,
    {
        let mut wrapper = self.wrapper.borrow_mut();
        let mut handle = wrapper.activate();
        handle.session(cap).give(data);
    }
}

struct TimelyWaker {
    activator: SyncActivator,
    active: AtomicBool,
    task_ready: AtomicBool,
}

impl futures_util::task::ArcWake for TimelyWaker {
    fn wake_by_ref(this: &Arc<Self>) {
        this.task_ready.store(true, Ordering::SeqCst);
        if !this.active.load(Ordering::SeqCst) {
            let _ = this.activator.activate();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::{AsCollection, Collection};
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Operator, ToStream};
    use tokio_stream::{StreamExt, iter};

    #[test]
    fn assert_async_output() {
        let captured = timely::example(|scope| {
            let mut builder =
                AsyncOperatorBuilder::new("assert_async_output".to_string(), scope.clone());
            let (out, out_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

            builder.build(move |caps| async move {
                let cap = &caps[0];

                let mut stream = iter(0u64..5);
                while let Some(n) = stream.next().await {
                    tokio::task::yield_now().await;
                    out.give(cap, n * 2);
                }
            });

            out_stream.capture()
        });

        let got = captured.extract();
        assert_eq!(got, vec![(0u64, vec![0, 2, 4, 6, 8])]);
    }

    #[test]
    fn assert_capability_downgrade() {
        let captured = timely::example(|scope| {
            let mut builder = AsyncOperatorBuilder::new("cap_downgrade".to_string(), scope.clone());
            let (out, out_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

            builder.build(move |mut caps| async move {
                let mut cap = caps.remove(0);
                out.give(&cap, 0u64);
                out.give(&cap, 1u64);

                tokio::task::yield_now().await;

                // Advance to timestamp 1
                cap.downgrade(&1u64);
                out.give(&cap, 10u64);
                out.give(&cap, 11u64);
            });

            out_stream.capture()
        });

        let got = captured.extract();
        assert_eq!(
            got,
            vec![
                // timestamp 0
                (0u64, vec![0, 1]),
                // timestamp 1
                (1u64, vec![10, 11])
            ]
        );
    }

    #[test]
    fn assert_two_outputs() {
        let (even_recv, odd_recv) = timely::example(|scope| {
            let mut builder = AsyncOperatorBuilder::new("two_outputs".to_string(), scope.clone());

            let (even_out, even_stream) =
                builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();
            let (odd_out, odd_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

            builder.build(move |caps| async move {
                let [even_cap, odd_cap]: [&Capability<_>; 2] = [&caps[0], &caps[1]];

                let mut stream = iter(0u64..10);
                while let Some(n) = stream.next().await {
                    if n % 2 == 0 {
                        even_out.give(even_cap, n);
                    } else {
                        odd_out.give(odd_cap, n);
                    }
                }
            });

            (even_stream.capture(), odd_stream.capture())
        });

        let even = even_recv.extract();
        let odd = odd_recv.extract();

        assert_eq!(even, vec![(0, vec![0, 2, 4, 6, 8])]);
        assert_eq!(odd, vec![(0, vec![1, 3, 5, 7, 9])]);
    }

    #[test]
    fn assert_async_with_input() {
        let captured = timely::example(|scope| {
            let input = (0u64..5).to_stream(scope);

            let mut builder =
                AsyncOperatorBuilder::new("async_with_input".to_string(), scope.clone());
            let (out, out_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();
            let mut in_handle = builder.new_input(&input);

            builder.build(move |caps| async move {
                let cap = &caps[0];
                for _ in 0..5 {
                    if let Some((_t, x)) = in_handle.next().await {
                        out.give(cap, x * 2);
                    }
                }
            });

            out_stream.capture()
        });

        let got = captured.extract();
        assert_eq!(got, vec![(0u64, vec![0, 2, 4, 6, 8])]);
    }

    #[test]
    fn assert_async_output_unary_test() {
        let captured = timely::example(|scope| {
            let mut builder = AsyncOperatorBuilder::new("async_unary".to_string(), scope.clone());

            let (out, out_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

            builder.build(move |caps| async move {
                let cap = &caps[0];
                for n in 0u64..5 {
                    tokio::task::yield_now().await;
                    out.give(cap, n * 2);
                }
            });

            let mapped: Collection<_, u64, isize> = out_stream
                .unary(Pipeline, "multiply_by_10", |_, _| {
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            let mut session = output.session(&time);
                            for d in data.drain(..) {
                                session.give((d * 10, *time, 1isize));
                            }
                        }
                    }
                })
                .as_collection();

            mapped.inner.capture()
        });

        let got = captured.extract();
        assert_eq!(
            got,
            vec![(
                0,
                vec![(0, 0, 1), (20, 0, 1), (40, 0, 1), (60, 0, 1), (80, 0, 1)]
            )]
        );
    }
}
