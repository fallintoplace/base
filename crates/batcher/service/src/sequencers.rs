//! The sequencer endpoints of the batcher, and which of them is the leader.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_provider::{Provider, RootProvider};
use base_common_network::Base;
use base_consensus_rpc::AdminApiClient;
use base_runtime::Runtime;
use futures::future;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use tracing::{info, warn};
use url::Url;

/// One sequencer endpoint, which serves the rollup node methods and the L2 blocks of its
/// sequencer.
#[derive(Debug)]
pub struct Sequencer {
    /// The endpoint origin, for logs and errors. It never carries a path or credentials.
    pub origin: String,
    /// Client of the rollup node methods, `optimism_*` and `admin_sequencerActive`.
    pub rollup_node_client: HttpClient,
    /// Provider of the L2 blocks.
    pub l2_provider: RootProvider<Base>,
}

impl Sequencer {
    /// Creates the clients of the sequencer endpoint at `url`, without sending a request.
    pub async fn new(url: &Url) -> eyre::Result<Self> {
        let origin = url.origin().ascii_serialization();

        let rollup_node_client = HttpClientBuilder::default()
            .build(url.as_str())
            .map_err(|e| eyre::eyre!("failed to build the sequencer client for {origin}: {e}"))?;
        let l2_provider = RootProvider::connect(url.as_str())
            .await
            .map_err(|e| eyre::eyre!("failed to build the L2 provider for {origin}: {e}"))?;

        Ok(Self { origin, rollup_node_client, l2_provider })
    }
}

/// The sequencer endpoints of the batcher, one of which is the leader that builds the blocks.
///
/// A single sequencer is the leader. Among several, the leader is the one whose
/// `admin_sequencerActive` answers `true`, which [`find_leader`](Self::find_leader) looks for.
///
/// As a [`Provider`], it sends each request to the leader of the moment.
#[derive(Debug)]
pub struct Sequencers {
    sequencers: Vec<Sequencer>,
    /// Index of the leader in `sequencers`.
    leader_index: AtomicUsize,
}

impl Sequencers {
    /// How long a sequencer has to answer `admin_sequencerActive`, which bounds how long an
    /// unresponsive sequencer delays [`find_leader`](Self::find_leader).
    pub const SEQUENCER_ACTIVE_TIMEOUT: Duration = Duration::from_secs(2);

    /// Creates the sequencers at `urls`. The first one is the leader until
    /// [`find_leader`](Self::find_leader) finds another.
    ///
    /// # Errors
    ///
    /// Returns an error when `urls` is empty or one of them is not an HTTP URL.
    pub async fn new(urls: &[Url]) -> eyre::Result<Self> {
        if urls.is_empty() {
            eyre::bail!("at least one sequencer URL is required");
        }
        let sequencers = future::try_join_all(urls.iter().map(Sequencer::new)).await?;
        Ok(Self { sequencers, leader_index: AtomicUsize::new(0) })
    }

    /// The current leader.
    pub fn leader(&self) -> &Sequencer {
        &self.sequencers[self.leader_index.load(Ordering::Relaxed)]
    }

    /// Asks every sequencer whether it is active and makes an active one the leader: the current
    /// leader when it still is, the first active one otherwise.
    ///
    /// A sequencer that answers `false`, answers an error or does not answer within
    /// [`SEQUENCER_ACTIVE_TIMEOUT`](Self::SEQUENCER_ACTIVE_TIMEOUT) is not active.
    ///
    /// # Errors
    ///
    /// Returns an error when no sequencer is active. The leader is then unchanged.
    pub async fn find_leader(&self) -> eyre::Result<()> {
        let answers = self.sequencers.iter().map(|sequencer| async {
            let answer = sequencer.rollup_node_client.admin_sequencer_active();
            matches!(
                tokio::time::timeout(Self::SEQUENCER_ACTIVE_TIMEOUT, answer).await,
                Ok(Ok(true))
            )
        });
        let active = future::join_all(answers).await;

        let active_count = active.iter().filter(|active| **active).count();
        if active_count > 1 {
            warn!(active_count, "several sequencers are active");
        }

        let current = self.leader_index.load(Ordering::Relaxed);
        if active[current] {
            return Ok(());
        }
        let Some(leader) = active.iter().position(|active| *active) else {
            eyre::bail!("no sequencer is active");
        };

        self.leader_index.store(leader, Ordering::Relaxed);
        info!(leader = %self.sequencers[leader].origin, "sequencer leader changed");
        Ok(())
    }

    /// Looks for the leader every `poll_interval` until `runtime` is cancelled.
    pub async fn track_leader<R: Runtime>(self: Arc<Self>, runtime: R, poll_interval: Duration) {
        loop {
            tokio::select! {
                biased;
                () = runtime.cancelled() => return,
                () = runtime.sleep(poll_interval) => {}
            }

            if let Err(error) = self.find_leader().await {
                warn!(
                    error = %error,
                    leader = %self.leader().origin,
                    "failed to find the sequencer leader, keeping the current one"
                );
            }
        }
    }
}

impl Provider<Base> for Sequencers {
    fn root(&self) -> &RootProvider<Base> {
        &self.leader().l2_provider
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, sync::Mutex};

    use base_runtime::{Cancellation, TokioRuntime};
    use jsonrpsee::{
        RpcModule,
        server::{Server, ServerHandle},
        types::ErrorObjectOwned,
    };

    use super::*;

    /// What a [`FakeSequencer`] answers to `admin_sequencerActive`.
    #[derive(Debug, Clone, Copy)]
    enum Activity {
        /// `true`, the answer of the leader.
        Active,
        /// `false`, the answer of a sequencer that is stopped.
        Stopped,
        /// An error, the answer of the conductor of a sequencer that is not the leader.
        NotLeader,
    }

    /// A sequencer endpoint whose answer to `admin_sequencerActive` the test sets, and which
    /// answers `eth_chainId` with its own `chain_id` so a test can tell which one served a read.
    ///
    /// It is a real JSON-RPC server because a client numbers its requests and these tests call
    /// the same endpoint several times, with an answer that changes in between. The server stops
    /// when the fake is dropped.
    struct FakeSequencer {
        url: Url,
        activity: Arc<Mutex<Activity>>,
        _server: ServerHandle,
    }

    impl FakeSequencer {
        async fn start(activity: Activity, chain_id: u64) -> Self {
            let activity = Arc::new(Mutex::new(activity));
            let mut module = RpcModule::new(Arc::clone(&activity));
            module
                .register_method("admin_sequencerActive", |_, activity, _| {
                    match *activity.lock().unwrap() {
                        Activity::Active => Ok(true),
                        Activity::Stopped => Ok(false),
                        Activity::NotLeader => {
                            Err(ErrorObjectOwned::owned(-32000, "not the leader", None::<()>))
                        }
                    }
                })
                .unwrap();
            module.register_method("eth_chainId", move |_, _, _| format!("{chain_id:#x}")).unwrap();

            let server = Server::builder().build("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", server.local_addr().unwrap()).parse().unwrap();
            Self { url, activity, _server: server.start(module) }
        }

        fn set(&self, activity: Activity) {
            *self.activity.lock().unwrap() = activity;
        }

        fn origin(&self) -> String {
            self.url.origin().ascii_serialization()
        }
    }

    /// The leader is the active sequencer. A sequencer that is stopped, one that is not the
    /// leader behind its conductor and one that is unreachable are passed over, even when they
    /// come first.
    #[tokio::test]
    async fn find_leader_makes_the_active_sequencer_the_leader() {
        let stopped = FakeSequencer::start(Activity::Stopped, 1).await;
        let not_leader = FakeSequencer::start(Activity::NotLeader, 2).await;
        let unreachable = "http://127.0.0.1:1".parse().unwrap();
        let active = FakeSequencer::start(Activity::Active, 3).await;
        let urls = [stopped.url.clone(), not_leader.url.clone(), unreachable, active.url.clone()];
        let sequencers = Sequencers::new(&urls).await.unwrap();

        sequencers.find_leader().await.unwrap();

        assert_eq!(sequencers.leader().origin, active.origin());
    }

    /// A sequencer that accepts the connection and never answers delays the search by the
    /// timeout of its answer, and is passed over.
    #[tokio::test]
    async fn find_leader_does_not_wait_for_a_sequencer_that_never_answers() {
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let silent_url = format!("http://{}", silent.local_addr().unwrap()).parse().unwrap();
        let active = FakeSequencer::start(Activity::Active, 1).await;
        let sequencers = Sequencers::new(&[silent_url, active.url.clone()]).await.unwrap();

        tokio::time::timeout(Sequencers::SEQUENCER_ACTIVE_TIMEOUT * 2, sequencers.find_leader())
            .await
            .expect("the search must not outlast the answer timeout")
            .unwrap();

        assert_eq!(sequencers.leader().origin, active.origin());
    }

    /// The leader stays the leader while it is active, even if a sequencer listed before it is
    /// active too.
    #[tokio::test]
    async fn find_leader_keeps_a_leader_that_is_still_active() {
        let first = FakeSequencer::start(Activity::Stopped, 1).await;
        let second = FakeSequencer::start(Activity::Active, 2).await;
        let sequencers = Sequencers::new(&[first.url.clone(), second.url.clone()]).await.unwrap();
        sequencers.find_leader().await.unwrap();

        first.set(Activity::Active);
        sequencers.find_leader().await.unwrap();

        assert_eq!(sequencers.leader().origin, second.origin());
    }

    /// Without an active sequencer the search fails and the leader stays the last one found.
    #[tokio::test]
    async fn find_leader_fails_and_keeps_the_leader_when_no_sequencer_is_active() {
        let first = FakeSequencer::start(Activity::Stopped, 1).await;
        let second = FakeSequencer::start(Activity::Active, 2).await;
        let sequencers = Sequencers::new(&[first.url.clone(), second.url.clone()]).await.unwrap();
        sequencers.find_leader().await.unwrap();

        second.set(Activity::NotLeader);
        let error = sequencers.find_leader().await.unwrap_err();

        assert_eq!(error.to_string(), "no sequencer is active");
        assert_eq!(sequencers.leader().origin, second.origin());
    }

    /// A read through the provider goes to the leader, and to the new leader once
    /// `track_leader` finds it.
    #[tokio::test]
    async fn reads_go_to_the_current_leader() {
        let first = FakeSequencer::start(Activity::Active, 1).await;
        let second = FakeSequencer::start(Activity::NotLeader, 2).await;
        let sequencers =
            Arc::new(Sequencers::new(&[first.url.clone(), second.url.clone()]).await.unwrap());
        let runtime = TokioRuntime::new();
        let tracking = tokio::spawn(
            Arc::clone(&sequencers).track_leader(runtime.clone(), Duration::from_millis(1)),
        );
        assert_eq!(sequencers.get_chain_id().await.unwrap(), 1);

        first.set(Activity::NotLeader);
        second.set(Activity::Active);
        tokio::time::timeout(Duration::from_secs(5), async {
            while sequencers.leader().origin != second.origin() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second sequencer must become the leader");

        assert_eq!(sequencers.get_chain_id().await.unwrap(), 2);
        runtime.cancel();
        tracking.await.unwrap();
    }
}
