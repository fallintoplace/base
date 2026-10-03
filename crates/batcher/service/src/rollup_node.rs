//! The rollup node whose derivation the batcher follows.

use std::sync::Arc;

use base_batcher_core::DerivationStatus;
use base_consensus_rpc::RollupNodeApiClient;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};

use crate::{DerivationStatusProvider, Sequencers, ShadowConfig};

/// The rollup node whose rollup config the batcher reads and whose derivation it follows.
#[derive(Debug)]
pub enum RollupNode {
    /// The rollup node of the leader sequencer, which a canonical batcher follows.
    Leader(Arc<Sequencers>),
    /// The rollup node of the parity validator, which a shadow batcher follows.
    ParityValidator(HttpClient),
}

impl RollupNode {
    /// The rollup node of the parity validator of `shadow`, or of the leader among `sequencers`
    /// for a canonical batcher. No request is sent.
    pub fn new(shadow: Option<&ShadowConfig>, sequencers: &Arc<Sequencers>) -> eyre::Result<Self> {
        let Some(shadow) = shadow else {
            return Ok(Self::Leader(Arc::clone(sequencers)));
        };

        let client = HttpClientBuilder::default()
            .build(shadow.validator_rollup_rpc_url.as_str())
            .map_err(|e| {
            eyre::eyre!("failed to build the client of the parity validator's rollup node: {e}")
        })?;
        Ok(Self::ParityValidator(client))
    }

    /// The client of the rollup node: for [`Leader`](Self::Leader), the client of the leader of
    /// the moment.
    pub fn client(&self) -> &HttpClient {
        match self {
            Self::Leader(sequencers) => &sequencers.leader().rollup_node_client,
            Self::ParityValidator(client) => client,
        }
    }
}

/// Reads the derivation status from the rollup node's `optimism_syncStatus`.
impl DerivationStatusProvider for RollupNode {
    async fn derivation_status(
        &self,
    ) -> Result<DerivationStatus, Box<dyn std::error::Error + Send + Sync>> {
        let status = self.client().sync_status().await?;
        Ok(DerivationStatus {
            safe_l2: status.local_safe_l2.block_info,
            current_l1: status.current_l1,
        })
    }
}
