// An asynchronous operator builder for timely dataflow.
// mostly from https://github.com/MaterializeInc/materialize/blob/main/src/timely-util/src/builder_async.rs
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker, ready};

use futures_util::Stream;

use timely::communication::Pull;
use timely::container::CapacityContainerBuilder;
use timely::container::PushInto;
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pushers::Output;
use timely::dataflow::operators::generic::InputHandleCore;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as TimelyOperatorBuilder;
use timely::dataflow::operators::{Capability, CapabilitySet, InputCapability};
use timely::dataflow::{Scope, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::SyncActivator;
use timely::{Container, ContainerBuilder, PartialOrder};

pub struct AsyncOperatorBuilder<G: Scope> {
    builder: TimelyOperatorBuilder<G>,
    input_frontiers: Vec<Antichain<G::Timestamp>>,
    output_flushes: Vec<Box<dyn FnMut()>>,
    input_queues: Vec<Box<dyn InputQueue<G::Timestamp>>>,
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
            input_frontiers: Default::default(),
            output_flushes: Default::default(),
            input_queues: Default::default(),
            waker_state,
        }
    }

    /// Adds a new input that is connected to the specified output, returning the async input handle to use.
    pub fn new_input_for<D, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        output: &dyn OutputIndex,
    ) -> AsyncInputHandle<G::Timestamp, D, ConnectedToOne>
    where
        D: Container + 'static,
        P: ParallelizationContract<G::Timestamp, D>,
    {
        let index = output.index();
        assert!(index < self.builder.shape().outputs());
        self.new_input_connection(stream, pact, ConnectedToOne(index))
    }

    /// Adds a new input that is connected to the specified outputs, returning the async input handle to use.
    pub fn new_input_for_many<const N: usize, D, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        outputs: [&dyn OutputIndex; N],
    ) -> AsyncInputHandle<G::Timestamp, D, ConnectedToMany<N>>
    where
        D: Container + 'static,
        P: ParallelizationContract<G::Timestamp, D>,
    {
        let indices = outputs.map(|output| output.index());
        for index in indices {
            assert!(index < self.builder.shape().outputs());
        }
        self.new_input_connection(stream, pact, ConnectedToMany(indices))
    }

    /// Adds a new input that is not connected to any output, returning the async input handle to use.
    pub fn new_disconnected_input<D, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
    ) -> AsyncInputHandle<G::Timestamp, D, Disconnected>
    where
        D: Container + 'static,
        P: ParallelizationContract<G::Timestamp, D>,
    {
        self.new_input_connection(stream, pact, Disconnected)
    }

    /// Adds a new input with connection information, returning the async input handle to use.
    pub fn new_input_connection<D, P, C>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        connection: C,
    ) -> AsyncInputHandle<G::Timestamp, D, C>
    where
        D: Container + 'static,
        P: ParallelizationContract<G::Timestamp, D>,
        C: InputConnection<G::Timestamp> + 'static,
    {
        self.input_frontiers
            .push(Antichain::from_elem(G::Timestamp::minimum()));

        let outputs = self.builder.shape().outputs();
        let handle = self.builder.new_input_connection(
            stream,
            pact,
            connection.describe(outputs).into_iter().enumerate(),
        );

        let waker = Default::default();
        let queue = Default::default();
        let input_queue = InputHandleQueue {
            queue: Rc::clone(&queue),
            waker: Rc::clone(&waker),
            connection,
            handle,
        };
        self.input_queues.push(Box::new(input_queue));

        AsyncInputHandle {
            queue,
            waker,
            done: false,
        }
    }

    pub fn new_output<CB>(
        &mut self,
    ) -> (
        AsyncOutputHandle<G::Timestamp, CB>,
        StreamCore<G, CB::Container>,
    )
    where
        CB: ContainerBuilder,
    {
        let index = self.builder.shape().outputs();

        let (output, stream) = self.builder.new_output_connection([]);

        let handle = AsyncOutputHandle::new(output, index);

        let flush_handle = handle.clone();
        self.output_flushes
            .push(Box::new(move || flush_handle.cease()));

        (handle, stream)
    }

    pub fn build<F, Fut>(mut self, logic: F)
    where
        F: FnOnce(Vec<Capability<G::Timestamp>>) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let waker_state = Arc::clone(&self.waker_state);
        let operator_waker = self.waker_state;
        let mut input_frontiers = self.input_frontiers;
        let mut input_queues = self.input_queues;
        let mut output_flushes = std::mem::take(&mut self.output_flushes);

        self.builder.build_reschedule(move |caps| {
            let mut fut = Some(Box::pin(logic(caps.clone())));
            let mut finished = false;
            let waker_state = Arc::clone(&waker_state);

            move |new_frontiers| {
                operator_waker.active.store(true, Ordering::SeqCst);
                for (i, queue) in input_queues.iter_mut().enumerate() {
                    // First, discover if there are any frontier notifications
                    let cur = &mut input_frontiers[i];
                    let new = new_frontiers[i].frontier();
                    if PartialOrder::less_than(&cur.borrow(), &new) {
                        queue.notify_progress(new.to_owned());
                        *cur = new.to_owned();
                    }
                    // Then accept all input into local queues. This step registers the received
                    // messages with progress tracking.
                    queue.accept_input();
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
                        for flush in output_flushes.iter_mut() {
                            (flush)();
                        }
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

/// Async handle to an operator's input stream
pub struct AsyncInputHandle<T: Timestamp, D: Container, C: InputConnection<T>> {
    #[allow(clippy::type_complexity)]
    queue: Rc<RefCell<VecDeque<Event<T, C::Capability, D>>>>,
    waker: Rc<Cell<Option<Waker>>>,
    /// Whether this handle has finished producing data
    done: bool,
}

impl<T: Timestamp, D: Container, C: InputConnection<T>> AsyncInputHandle<T, D, C> {
    pub fn next_sync(&mut self) -> Option<Event<T, C::Capability, D>> {
        let mut queue = self.queue.borrow_mut();
        match queue.pop_front()? {
            Event::Data(cap, data) => Some(Event::Data(cap, data)),
            Event::Progress(frontier) => {
                self.done = frontier.is_empty();
                Some(Event::Progress(frontier))
            }
        }
    }

    /// Waits for the handle to have data. After this function returns it is guaranteed that at
    /// least one call to `next_sync` will be `Some(_)`.
    pub async fn ready(&self) {
        std::future::poll_fn(|cx| self.poll_ready(cx)).await
    }

    fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        if self.queue.borrow().is_empty() {
            self.waker.set(Some(cx.waker().clone()));
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl<T: Timestamp, D: Container, C: InputConnection<T>> Stream for AsyncInputHandle<T, D, C> {
    type Item = Event<T, C::Capability, D>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        ready!(self.poll_ready(cx));
        Poll::Ready(self.next_sync())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.queue.borrow().len(), None)
    }
}

/// Shared part of an async output handle
struct AsyncOutputHandleInner<T: Timestamp, CB: ContainerBuilder> {
    /// Handle to write to the output stream.
    output: Output<T, CB::Container>,
    /// Current capability held by this output handle.
    capability: Option<Capability<T>>,
    /// Container builder to accumulate data before sending at `capability`.
    builder: CB,
}

impl<T: Timestamp, CB: ContainerBuilder> AsyncOutputHandleInner<T, CB> {
    /// Write all pending data to the output stream.
    fn flush(&mut self) {
        while let Some(container) = self.builder.finish() {
            self.output
                .give(self.capability.as_ref().expect("must exist"), container);
        }
    }

    /// Cease this output handle, flushing all pending data and releasing its capability.
    fn cease(&mut self) {
        self.flush();
        let _ = self.output.activate();
        self.capability = None;
    }

    /// Provides data at the time specified by the capability. Flushes automatically when the
    /// capability time changes.
    fn give<D>(&mut self, cap: &Capability<T>, data: D)
    where
        CB: PushInto<D>,
    {
        if let Some(capability) = &self.capability
            && cap.time() != capability.time()
        {
            self.flush();
            self.capability = None;
        }
        if self.capability.is_none() {
            self.capability = Some(cap.clone());
        }

        self.builder.push_into(data);
        while let Some(container) = self.builder.extract() {
            self.output
                .give(self.capability.as_ref().expect("must exist"), container);
        }
    }
}

pub struct AsyncOutputHandle<T: Timestamp, CB: ContainerBuilder> {
    inner: Rc<RefCell<AsyncOutputHandleInner<T, CB>>>,
    index: usize,
}

impl<T, C> AsyncOutputHandle<T, CapacityContainerBuilder<C>>
where
    T: Timestamp,
    C: Container + Clone + 'static,
{
    #[inline]
    pub fn give_container(&self, cap: &Capability<T>, container: &mut C) {
        let mut inner = self.inner.borrow_mut();
        inner.flush();
        inner.output.give(cap, container);
    }
}

impl<T, CB> AsyncOutputHandle<T, CB>
where
    T: Timestamp,
    CB: ContainerBuilder,
{
    fn new(output: Output<T, CB::Container>, index: usize) -> Self {
        let inner = AsyncOutputHandleInner {
            output,
            capability: None,
            builder: CB::default(),
        };
        Self {
            inner: Rc::new(RefCell::new(inner)),
            index,
        }
    }

    fn cease(&self) {
        self.inner.borrow_mut().cease();
    }
}

impl<T, CB> AsyncOutputHandle<T, CB>
where
    T: Timestamp,
    CB: ContainerBuilder,
{
    pub fn give<D>(&self, cap: &Capability<T>, data: D)
    where
        CB: PushInto<D>,
    {
        self.inner.borrow_mut().give(cap, data);
    }
}

impl<T: Timestamp, CB: ContainerBuilder> Clone for AsyncOutputHandle<T, CB> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            index: self.index,
        }
    }
}

/// A helper trait abstracting over an output handle. It facilitates passing type erased
/// output handles during operator construction.
/// It is not meant to be implemented by users.
pub trait OutputIndex {
    /// The output index of this handle.
    fn index(&self) -> usize;
}

impl<T: Timestamp, CB: ContainerBuilder> OutputIndex for AsyncOutputHandle<T, CB> {
    fn index(&self) -> usize {
        self.index
    }
}

/// A trait describing the connection behavior between an input of an operator and zero or more of
/// its outputs.
pub trait InputConnection<T: Timestamp> {
    /// The capability type associated with this connection behavior.
    type Capability;

    /// Generates a summary description of the connection behavior given the number of outputs.
    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>>;

    /// Accepts an input capability.
    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability;
}

/// A marker type representing a disconnected input.
pub struct Disconnected;

impl<T: Timestamp> InputConnection<T> for Disconnected {
    type Capability = T;

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        vec![Antichain::new(); outputs]
    }

    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability {
        input_cap.time().clone()
    }
}

/// A marker type representing an input connected to exactly one output.
pub struct ConnectedToOne(usize);

impl<T: Timestamp> InputConnection<T> for ConnectedToOne {
    type Capability = Capability<T>;

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        let mut summary = vec![Antichain::new(); outputs];
        summary[self.0] = Antichain::from_elem(T::Summary::default());
        summary
    }

    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability {
        input_cap.retain_for_output(self.0)
    }
}

/// A marker type representing an input connected to many outputs.
pub struct ConnectedToMany<const N: usize>([usize; N]);

impl<const N: usize, T: Timestamp> InputConnection<T> for ConnectedToMany<N> {
    type Capability = [Capability<T>; N];

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        let mut summary = vec![Antichain::new(); outputs];
        for output in self.0 {
            summary[output] = Antichain::from_elem(T::Summary::default());
        }
        summary
    }

    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability {
        self.0
            .map(|output| input_cap.delayed_for_output(input_cap.time(), output))
    }
}

/// An event of an input stream
#[derive(Debug)]
pub enum Event<T: Timestamp, C, D> {
    /// A data event
    Data(C, D),
    /// A progress event
    Progress(Antichain<T>),
}

/// A helper trait abstracting over an input handle. It facilitates keeping around type erased
/// handles for each of the operator inputs.
trait InputQueue<T: Timestamp> {
    /// Accepts all available input into local queues.
    fn accept_input(&mut self);

    /// Drains all available input and empties the local queue.
    #[allow(dead_code)]
    fn drain_input(&mut self);

    /// Registers a frontier notification to be delivered.
    fn notify_progress(&mut self, upper: Antichain<T>);
}

impl<T, D, C, P> InputQueue<T> for InputHandleQueue<T, D, C, P>
where
    T: Timestamp,
    D: Container,
    C: InputConnection<T> + 'static,
    P: Pull<Message<T, D>> + 'static,
{
    fn accept_input(&mut self) {
        let mut queue = self.queue.borrow_mut();
        let mut new_data = false;
        while let Some((cap, data)) = self.handle.next() {
            new_data = true;
            let cap = self.connection.accept(cap);
            queue.push_back(Event::Data(cap, std::mem::take(data)));
        }
        if new_data && let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn drain_input(&mut self) {
        self.queue.borrow_mut().clear();
        self.handle.for_each(|_, _| {});
    }

    fn notify_progress(&mut self, upper: Antichain<T>) {
        let mut queue = self.queue.borrow_mut();
        // It's beneficial to consolidate two consecutive progress statements into one if the
        // operator hasn't seen the previous progress yet. This also avoids accumulation of
        // progress statements in the queue if the operator only conditionally checks this input.
        match queue.back_mut() {
            Some(&mut Event::Progress(ref mut prev_upper)) => *prev_upper = upper,
            _ => queue.push_back(Event::Progress(upper)),
        }
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

struct InputHandleQueue<
    T: Timestamp,
    D: Container,
    C: InputConnection<T>,
    P: Pull<Message<T, D>> + 'static,
> {
    queue: Rc<RefCell<VecDeque<Event<T, C::Capability, D>>>>,
    waker: Rc<Cell<Option<Waker>>>,
    connection: C,
    handle: InputHandleCore<T, D, P>,
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
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, ToStream};
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
    fn assert_async_with_input_for() {
        let captured = timely::example(|scope| {
            let input = (0u64..5).to_stream(scope);

            let mut builder =
                AsyncOperatorBuilder::new("async_with_input_for".to_string(), scope.clone());

            let (out, out_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();
            let mut input = builder.new_input_for(&input, Pipeline, &out);

            builder.build(move |caps| async move {
                let cap = &caps[0];

                while let Some(event) = input.next().await {
                    match event {
                        Event::Data(_cap_from_input, data) => {
                            for x in data.into_iter() {
                                out.give(cap, x * 2);
                            }
                        }
                        Event::Progress(frontier) => {
                            if frontier.is_empty() {
                                break;
                            }
                        }
                    }
                }
            });

            out_stream.capture()
        });

        let got = captured.extract();
        assert_eq!(got, vec![(0u64, vec![0, 2, 4, 6, 8])]);
    }

    #[test]
    fn assert_disconnected_input() {
        let (captured, saw_empty_frontier) = timely::example(|scope| {
            let input = (0u64..5).to_stream(scope);

            let mut builder =
                AsyncOperatorBuilder::new("async_disconnected_input".to_string(), scope.clone());
            let (out, out_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();
            let mut input = builder.new_disconnected_input(&input, Pipeline);

            let saw_empty = Arc::new(AtomicBool::new(false));
            let saw_empty_clone = Arc::clone(&saw_empty);

            builder.build(move |caps| async move {
                let cap = &caps[0];

                while let Some(event) = input.next().await {
                    match event {
                        Event::Data(_time, data) => {
                            for x in data.into_iter() {
                                out.give(cap, x * 2);
                            }
                        }
                        Event::Progress(frontier) => {
                            if frontier.is_empty() {
                                saw_empty_clone.store(true, Ordering::SeqCst);
                            }
                        }
                    }
                }
            });

            (out_stream.capture(), saw_empty)
        });

        let got = captured.extract();
        assert_eq!(got, vec![(0u64, vec![0, 2, 4, 6, 8])]);
        assert!(saw_empty_frontier.load(Ordering::SeqCst));
    }

    #[test]
    fn assert_input_for_many() {
        let (even_captured, odd_captured) = timely::example(|scope| {
            let input = (0u64..10).to_stream(scope);

            let mut builder =
                AsyncOperatorBuilder::new("async_with_input_for_many".to_string(), scope.clone());

            let (even_out, even_stream) =
                builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();
            let (odd_out, odd_stream) = builder.new_output::<CapacityContainerBuilder<Vec<u64>>>();

            let mut input = builder.new_input_for_many(
                &input,
                Pipeline,
                [&even_out as &dyn OutputIndex, &odd_out as &dyn OutputIndex],
            );

            builder.build(move |_caps| async move {
                while let Some(event) = input.next().await {
                    match event {
                        Event::Data(output_caps, data) => {
                            let even_cap = &output_caps[0];
                            let odd_cap = &output_caps[1];

                            for value in data.into_iter() {
                                if value % 2 == 0 {
                                    even_out.give(even_cap, value);
                                } else {
                                    odd_out.give(odd_cap, value);
                                }
                            }
                        }
                        Event::Progress(frontier) => {
                            if frontier.is_empty() {
                                break;
                            }
                        }
                    }
                }
            });

            (even_stream.capture(), odd_stream.capture())
        });

        let even = even_captured.extract();
        let odd = odd_captured.extract();

        assert_eq!(even, vec![(0u64, vec![0, 2, 4, 6, 8])]);
        assert_eq!(odd, vec![(0u64, vec![1, 3, 5, 7, 9])]);
    }
}
