//! Simulation beyond a single `eth_call`: state overrides, sequences of calls across simulated
//! blocks (`eth_simulateV1`), and geth-style traces (`debug_traceCall`).
//!
//! All of it runs on a copy-on-write overlay (`CacheDB`) over skim's snapshot, so calls see each
//! other's writes and the store is never modified. Unknown state is handled as for `eth_call`:
//! a run that reads anything unknown is discarded, the missing keys are fetched (verified), and
//! the whole simulation re-runs.

use alloy_consensus::Header;
use alloy_primitives::U256;
use alloy_rpc_types_eth::{BlockOverrides, state::StateOverride};
use alloy_rpc_types_trace::geth::{
    GethDebugBuiltInTracerType, GethDebugTracerType, GethDebugTracingOptions, GethTrace, NoopFrame,
};
use revm::{
    Context, DatabaseCommit, ExecuteEvm, InspectEvm, MainBuilder, MainContext,
    bytecode::Bytecode,
    context::{BlockEnv, TxEnv, result::ExecutionResult},
    database::CacheDB,
    state::AccountInfo,
};
use revm_inspectors::tracing::{TracingInspector, TracingInspectorConfig};
use skim_state::{BlockRef, Snapshot};
use skim_snap::Priority;

use crate::{SimError, Simulator, db, env, prefetch};

/// One simulated block: optional environment and state overrides, then calls in order.
#[derive(Debug, Clone, Default)]
pub struct SimBlock {
    pub block: Option<BlockOverrides>,
    pub state: Option<StateOverride>,
    pub calls: Vec<TxEnv>,
}

#[derive(Debug, Clone)]
pub struct SimBlockResult {
    pub env: BlockEnv,
    pub calls: Vec<ExecutionResult>,
}

/// Where simulated blocks sit: on the base block's own environment (`eth_call` with overrides),
/// or as new blocks after it (`eth_simulateV1`: base+1, base+2, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    SameBlock,
    NextBlocks,
}

const MAX_ROUNDS: usize = 8;

fn apply_block_overrides(env: &mut BlockEnv, o: &BlockOverrides) {
    if let Some(n) = o.number {
        env.number = n;
    }
    if let Some(t) = o.time {
        env.timestamp = U256::from(t);
    }
    if let Some(g) = o.gas_limit {
        env.gas_limit = g;
    }
    if let Some(c) = o.coinbase {
        env.beneficiary = c;
    }
    if let Some(r) = o.random {
        env.prevrandao = Some(r);
    }
    if let Some(b) = o.base_fee {
        env.basefee = b.saturating_to();
    }
}

/// Applies account overrides to the overlay. Partial overrides start from the account's
/// current state, which may be unknown (recorded as a miss like any other read).
fn apply_state_overrides(cache: &mut CacheDB<&db::SnapDb<'_>>, o: &StateOverride) -> Result<(), SimError> {
    use revm::Database;
    for (addr, acc) in o.iter() {
        if acc.move_precompile_to.is_some() {
            return Err(SimError::Unsupported("movePrecompileToAddress"));
        }
        let mut info = cache.basic(*addr).map_err(|e| SimError::Evm(format!("{e:?}")))?.unwrap_or_default();
        if let Some(b) = acc.balance {
            info.balance = b;
        }
        if let Some(n) = acc.nonce {
            info.nonce = n;
        }
        if let Some(code) = &acc.code {
            let bc = Bytecode::new_raw(code.clone());
            info.code_hash = bc.hash_slow();
            info.code = Some(bc);
        }
        cache.insert_account_info(*addr, AccountInfo { ..info });
        if let Some(state) = &acc.state {
            // Full replacement: every slot not listed reads as zero.
            let slots = state.iter().map(|(k, v)| (U256::from_be_bytes(k.0), U256::from_be_bytes(v.0))).collect();
            cache.replace_account_storage(*addr, slots).map_err(|e| SimError::Evm(format!("{e:?}")))?;
        }
        if let Some(diff) = &acc.state_diff {
            for (k, v) in diff {
                cache
                    .insert_account_storage(*addr, U256::from_be_bytes(k.0), U256::from_be_bytes(v.0))
                    .map_err(|e| SimError::Evm(format!("{e:?}")))?;
            }
        }
    }
    Ok(())
}

impl Simulator {
    fn snapshot_ctx(&self, b: &BlockRef) -> Result<(Snapshot, std::sync::Arc<Header>), SimError> {
        let snap = self.store.snapshot(b)?;
        let header = self.header_arc(b).ok_or(SimError::NoHeader(b.number))?;
        Ok((snap, header))
    }

    /// One pass over all blocks on a fresh overlay. Returns the results and what was read
    /// (including misses, which invalidate the pass).
    fn simulate_once(&self, snap: &Snapshot, header: &Header, blocks: &[SimBlock], place: Placement) -> Result<(Vec<SimBlockResult>, db::Access), SimError> {
        let store = self.store.clone();
        let block_hash = move |n: u64| store.block_hash(n);
        let sdb = db::SnapDb { snap, block_hash: &block_hash, access: Default::default(), code_cache: &self.code_cache };
        let mut cache = CacheDB::new(&sdb);
        let base = env::block_env(header);
        let mut out = Vec::with_capacity(blocks.len());
        for (i, blk) in blocks.iter().enumerate() {
            let mut benv = base.clone();
            if place == Placement::NextBlocks {
                benv.number = base.number + U256::from(i as u64 + 1);
                benv.timestamp = base.timestamp + U256::from(12 * (i as u64 + 1));
                benv.basefee = 0; // no fee validation: simulated blocks don't charge a base fee
            }
            if let Some(o) = &blk.block {
                apply_block_overrides(&mut benv, o);
            }
            if let Some(s) = &blk.state {
                apply_state_overrides(&mut cache, s)?;
            }
            let mut results = Vec::with_capacity(blk.calls.len());
            for tx in &blk.calls {
                let r = {
                    let mut evm = Context::mainnet().with_db(&mut cache).with_block(benv.clone()).with_cfg(env::call_cfg()).build_mainnet();
                    evm.transact(tx.clone()).map_err(|e| SimError::Evm(format!("{e:?}")))?
                };
                cache.commit(r.state);
                results.push(r.result);
            }
            out.push(SimBlockResult { env: benv, calls: results });
        }
        drop(cache);
        let mut access = sdb.access.into_inner().unwrap();
        access.dedup();
        Ok((out, access))
    }

    /// Runs `blocks` of calls on an overlay over `b`'s state, fetching whatever they read that
    /// skim doesn't know (verified) and re-running until a pass reads only known state.
    pub async fn simulate(&self, b: &BlockRef, blocks: &[SimBlock], place: Placement) -> Result<Vec<SimBlockResult>, SimError> {
        let (snap, header) = self.snapshot_ctx(b)?;
        let at = if self.miss_at_parent { self.store.block(b.number.saturating_sub(1)).unwrap_or(*b) } else { *b };
        let retention = *self.on_demand.read();
        for _ in 0..MAX_ROUNDS {
            let (out, access) = self.simulate_once(&snap, &header, blocks, place)?;
            if !access.has_misses() {
                return Ok(out);
            }
            if snap.is_archived() {
                return Err(SimError::NotArchived { block: b.number, keys: access.miss_accounts.len() + access.miss_storage.len() });
            }
            if let Some(&lowest) = access.miss_block_hashes.first() {
                self.resolve_block_hashes(lowest).await;
            }
            prefetch::resolve_retained(&self.store, &self.sched, &at, &access.miss_accounts, &access.miss_storage, &access.miss_code, Priority::High, retention)
                .await;
        }
        Err(SimError::Incomplete(MAX_ROUNDS))
    }

    /// `debug_traceCall`: geth tracers (default struct logger, `callTracer`, `prestateTracer`,
    /// `noopTracer`) over `tx` at `b`, with optional state overrides.
    pub async fn trace_call(&self, b: &BlockRef, tx: TxEnv, overrides: Option<StateOverride>, opts: GethDebugTracingOptions) -> Result<GethTrace, SimError> {
        // Make every key the call reads local first (same resolution as eth_call); prestate
        // traces also report the fee recipient, so fetch that account too.
        let blocks = [SimBlock { block: None, state: overrides.clone(), calls: vec![tx.clone()] }];
        self.simulate(b, &blocks, Placement::SameBlock).await?;
        if matches!(opts.tracer, Some(GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer))) {
            let cb = self.header_arc(b).map(|h| h.beneficiary).unwrap_or_default();
            self.read(b, skim_bal::StateKey::Balance(cb)).await?;
            let _ = self.code(b, cb).await?;
        }
        let (snap, header) = self.snapshot_ctx(b)?;
        let store = self.store.clone();
        let block_hash = move |n: u64| store.block_hash(n);
        let sdb = db::SnapDb { snap: &snap, block_hash: &block_hash, access: Default::default(), code_cache: &self.code_cache };
        let mut cache = CacheDB::new(&sdb);
        if let Some(o) = &overrides {
            apply_state_overrides(&mut cache, o)?;
        }
        let benv = env::block_env(&header);
        let coinbase = benv.beneficiary;
        let gas_limit = tx.gas_limit;
        let tracer = opts.tracer.clone();
        let (insp_cfg, kind) = match &tracer {
            None => (TracingInspectorConfig::from_geth_config(&opts.config), 0),
            Some(GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::CallTracer)) => {
                let c = opts.tracer_config.clone().into_call_config().map_err(|e| SimError::Evm(format!("callTracer config: {e}")))?;
                (TracingInspectorConfig::from_geth_call_config(&c), 1)
            }
            Some(GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::PreStateTracer)) => {
                (TracingInspectorConfig::default_geth().set_record_logs(false), 2)
            }
            Some(GethDebugTracerType::BuiltInTracer(GethDebugBuiltInTracerType::NoopTracer)) => return Ok(GethTrace::NoopTracer(NoopFrame::default())),
            Some(_) => return Err(SimError::Unsupported("this tracer (supported: default, callTracer, prestateTracer, noopTracer)")),
        };
        let mut evm = Context::mainnet()
            .with_db(&mut cache)
            .with_block(benv)
            .with_cfg(env::call_cfg())
            .build_mainnet_with_inspector(TracingInspector::new(insp_cfg));
        let res = evm.inspect_tx(tx).map_err(|e| SimError::Evm(format!("{e:?}")))?;
        let insp = std::mem::replace(&mut evm.inspector, TracingInspector::new(TracingInspectorConfig::none()));
        drop(evm);
        {
            let a = sdb.access.lock().unwrap();
            if a.has_misses() {
                // State moved between resolution and tracing (a new block): refuse, never guess.
                return Err(SimError::StateUnavailable(a.miss_accounts.len() + a.miss_storage.len() + a.miss_code.len()));
            }
        }
        let gas = *res.result.gas();
        let output = res.result.output().cloned().unwrap_or_default();
        let builder = insp.into_geth_builder();
        Ok(match kind {
            0 => GethTrace::Default(builder.geth_traces_with_result_gas(gas, output, opts.config)),
            1 => {
                let c = opts.tracer_config.into_call_config().unwrap_or_default();
                let mut frame = builder.geth_call_traces_with_result_gas(c, gas);
                // geth/Nethermind report the transaction's gas limit on the root frame (not what
                // was left after intrinsic gas).
                frame.gas = U256::from(gas_limit);
                GethTrace::CallTracer(frame)
            }
            _ => {
                let c = opts.tracer_config.into_pre_state_config().map_err(|e| SimError::Evm(format!("prestateTracer config: {e}")))?;
                let mut frame = builder.geth_prestate_traces(&res, &c, &cache).map_err(|e| SimError::Evm(format!("{e:?}")))?;
                // geth and Nethermind always list the block's fee recipient (the tx pays it). skim's
                // calls charge no fees, so add it the way they report it.
                if let alloy_rpc_types_trace::geth::PreStateFrame::Default(mode) = &mut frame
                    && !mode.0.contains_key(&coinbase)
                {
                    use revm::DatabaseRef;
                    if let Ok(Some(info)) = cache.basic_ref(coinbase) {
                        let code = if info.code_hash == revm::primitives::KECCAK_EMPTY {
                            None
                        } else {
                            cache.code_by_hash_ref(info.code_hash).ok().map(|c| c.original_bytes())
                        };
                        mode.0.insert(coinbase, alloy_rpc_types_trace::geth::AccountState {
                            balance: Some(info.balance),
                            nonce: Some(info.nonce),
                            code,
                            ..Default::default()
                        });
                    }
                }
                GethTrace::PreStateTracer(frame)
            }
        })
    }
}
