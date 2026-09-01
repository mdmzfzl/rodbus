use std::future::Future;

use tokio_util::sync::CancellationToken;

/// Cancellation signal shared by a public handle and its background task.
#[derive(Clone, Debug, Default)]
pub(crate) struct TaskCancellation {
    token: CancellationToken,
}

impl TaskCancellation {
    pub(crate) fn cancel(&self) {
        self.token.cancel();
    }

    /// Run a future until it completes or cancellation is requested.
    ///
    /// Cancellation takes priority. When it wins, the operation is dropped before this returns.
    pub(crate) async fn run_until_cancelled<F>(&self, operation: F) -> Option<F::Output>
    where
        F: Future,
    {
        tokio::select! {
            biased;
            _ = self.token.cancelled() => None,
            result = operation => Some(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use super::*;

    struct PanicOnPoll;

    impl Future for PanicOnPoll {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("operation was polled after cancellation")
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancellation_wins_without_polling_the_operation() {
        let cancellation = TaskCancellation::default();
        cancellation.cancel();

        assert_eq!(cancellation.run_until_cancelled(PanicOnPoll).await, None);
    }

    #[tokio::test]
    async fn cancellation_drops_a_pending_operation() {
        let cancellation = TaskCancellation::default();
        let handle = cancellation.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = DropFlag(dropped.clone());

        let task = tokio::spawn(async move {
            cancellation
                .run_until_cancelled(async move {
                    let _flag = flag;
                    std::future::pending::<()>().await;
                })
                .await
        });

        tokio::task::yield_now().await;
        handle.cancel();

        assert_eq!(task.await.unwrap(), None);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
