use crate::common::phys::PhysLayer;
use crate::server::task::SessionTask;
use crate::server::RequestHandler;
use crate::{RequestError, RetryStrategy, SerialSettings, Shutdown};
use tokio_util::sync::CancellationToken;

pub(crate) struct RtuServerTask<T>
where
    T: RequestHandler,
{
    pub(crate) port: String,
    pub(crate) retry: Box<dyn RetryStrategy>,
    pub(crate) settings: SerialSettings,
    pub(crate) session: SessionTask<T>,
    pub(crate) shutdown: CancellationToken,
}

impl<T> RtuServerTask<T>
where
    T: RequestHandler,
{
    pub(crate) async fn run(&mut self) -> Shutdown {
        // the clone releases the borrow of self that run_inner() needs mutably
        let shutdown = self.shutdown.clone();
        tokio::select! {
            // biased so that a cancelled task cannot be given another poll of run_inner(), which
            // would let it finish work that is already ready before noticing the shutdown
            biased;
            _ = shutdown.cancelled() => Shutdown,
            res = self.run_inner() => res,
        }
    }

    async fn run_inner(&mut self) -> Shutdown {
        loop {
            match crate::serial::open(&self.port, self.settings) {
                Ok(serial) => {
                    self.retry.reset();
                    tracing::info!("opened port");
                    // run an open port until shutdown or failure
                    let mut phys = PhysLayer::new_serial(serial);
                    if let RequestError::Shutdown = self.session.run(&mut phys).await {
                        return Shutdown;
                    }
                    // we wait here to prevent any kind of rapid retry scenario if the port opens and immediately fails
                    let delay = self.retry.after_disconnect();
                    tracing::warn!("waiting {:?} to reopen port", delay);
                    if let Err(Shutdown) = self.session.sleep_for(delay).await {
                        return Shutdown;
                    }
                }
                Err(err) => {
                    let delay = self.retry.after_failed_connect();
                    tracing::warn!(
                        "unable to open serial port, retrying in {:?} - error: {}",
                        delay,
                        err
                    );
                    if let Err(Shutdown) = self.session.sleep_for(delay).await {
                        return Shutdown;
                    }
                }
            }
        }
    }
}
