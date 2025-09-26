use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{FusedStream, Stream, TryFuture, TryStream, ready};
use pin_project::pin_project;

#[pin_project]
struct BatchMap<S, F, T, Fut, U> {
    #[pin]
    stream: S,
    #[pin]
    fut: Option<Fut>,
    f: F,
    in_: Vec<T>,
    out: VecDeque<U>,
}

impl<
    S: FusedStream + TryStream<Ok = T, Error = E>,
    F: FnMut(Vec<T>) -> Fut,
    Fut: TryFuture<Ok: IntoIterator<Item = U>, Error = E>,
    U,
    T,
    E,
> Stream for BatchMap<S, F, T, Fut, U>
{
    type Item = Result<U, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(item) = this.out.pop_front() {
            return Poll::Ready(Some(Ok(item)));
        }
        while !this.stream.is_terminated()
            && let Poll::Ready(Some(item)) = this.stream.as_mut().try_poll_next(cx)?
        {
            this.in_.push(item);
        }
        if !this.in_.is_empty() && this.fut.is_none() {
            this.fut.set(Some((this.f)(std::mem::take(this.in_))));
        }
        if let Some(fut) = this.fut.as_mut().as_pin_mut() {
            this.out.extend(ready!(fut.try_poll(cx))?);
            this.fut.set(None);
        }
        if let Some(item) = this.out.pop_front() {
            Poll::Ready(Some(Ok(item)))
        } else if this.stream.is_terminated() && this.fut.is_none() {
            assert!(this.in_.is_empty());
            assert!(this.out.is_empty());
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<
    S: FusedStream + TryStream<Ok = T, Error = E>,
    F: FnMut(Vec<T>) -> Fut,
    Fut: TryFuture<Ok: IntoIterator<Item = U>, Error = E>,
    U,
    T,
    E,
> FusedStream for BatchMap<S, F, T, Fut, U>
{
    fn is_terminated(&self) -> bool {
        if self.fut.is_none() {
            assert!(self.in_.is_empty());
        }
        self.stream.is_terminated() && self.fut.is_none() && self.out.is_empty()
    }
}

pub fn batch_map<T, U, E, Fut: TryFuture<Ok: IntoIterator<Item = U>, Error = E>>(
    stream: impl FusedStream + TryStream<Ok = T, Error = E>,
    f: impl FnMut(Vec<T>) -> Fut,
) -> impl FusedStream<Item = Result<U, E>> {
    BatchMap {
        stream,
        fut: None,
        f,
        in_: Default::default(),
        out: Default::default(),
    }
}
