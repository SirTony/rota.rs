use std::future::Future;

use async_trait::async_trait;
use tokio::{select, task::JoinHandle, time::Sleep};
use tokio_util::sync::CancellationToken;

use crate::Error;

#[async_trait]
pub(crate) trait FutureEx: Future {
    async fn with_cancellation(mut self, ct: CancellationToken) -> Result<(), Error>;
}

#[async_trait]
impl FutureEx for JoinHandle<Result<(), Error>> {
    async fn with_cancellation(mut self, ct: CancellationToken) -> Result<(), Error> {
        select! {
            join_result = self => match join_result {
                Ok( task_result ) => match task_result {
                    Ok( x ) => Ok( x ),
                    Err( e ) => Err( e )
                },
                Err( _ ) => Err( Error::Cancelled )
            },
            _ = ct.cancelled() => Err( Error::Cancelled )
        }
    }
}

#[async_trait]
impl FutureEx for Sleep {
    async fn with_cancellation(mut self, ct: CancellationToken) -> Result<(), Error> {
        select! {
            sleep_result = self => Ok( sleep_result ),
            _ = ct.cancelled() => Err( Error::Cancelled )
        }
    }
}
