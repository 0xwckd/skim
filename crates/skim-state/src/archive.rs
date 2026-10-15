//! Selective state archive (PROJECT.md §7.6, M4): every version of *archived* keys, on disk.
//!
//! BALs carry absolute post-values, so the archive stores a key's base value (when it became
//! known) and then only the blocks that changed it:
//!
//! ```text
//! (slot X, 100) -> 10   base, coverage starts at 100
//! (slot X, 102) -> 20   BAL write; 101 and 103 store nothing
//! ```
//!
//! A read of key `K` at block `N` is answered only if:
//! - `N` lies in a **segment**: a run of consecutive blocks whose BALs were all processed
//!   (restarts without gap fill end a segment)
//! - `K` is **covered** at `N`: its value was known at or before `N` within that segment
//!
//! Otherwise it is [`ArchiveRead::NotCovered`], never a guess. Reorgs truncate exactly what the
//! orphaned blocks wrote, using a per-block journal.

use std::{collections::HashSet, path::Path};

use alloy_primitives::{Address, B256, Bytes, U256};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::StateKey;

/// Encoded state key: tag (1) + address (20) + slot (32).
const K: usize = 53;
/// Version key: encoded state key + big-endian block number.
const VK: usize = K + 8;

const VERSIONS: TableDefinition<&[u8; VK], &[u8; 32]> = TableDefinition::new("versions");
const COVERAGE: TableDefinition<&[u8; K], u64> = TableDefinition::new("coverage");
const ARCHIVED: TableDefinition<&[u8; K], ()> = TableDefinition::new("archived");
const HEADERS: TableDefinition<u64, &[u8]> = TableDefinition::new("headers");
const HASHES: TableDefinition<u64, &[u8; 32]> = TableDefinition::new("hashes");
const BY_HASH: TableDefinition<&[u8; 32], u64> = TableDefinition::new("by_hash");
const CODE: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("code");
const JOURNAL: TableDefinition<u64, &[u8]> = TableDefinition::new("journal");
/// Segments: first block -> last block (inclusive) of each run of consecutive processed blocks.
const SEGMENTS: TableDefinition<u64, u64> = TableDefinition::new("segments");

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("archive db: {0}")]
    Db(String),
    #[error("block {0} does not extend the archive (last {1:?})")]
    NotNext(u64, Option<u64>),
    #[error("gap fill does not fit: {0}")]
    GapMismatch(String),
}

macro_rules! db_err {
    ($($t:ty),*) => { $(impl From<$t> for ArchiveError { fn from(e: $t) -> Self { Self::Db(e.to_string()) } })* };
}
db_err!(redb::DatabaseError, redb::TransactionError, redb::TableError, redb::StorageError, redb::CommitError);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveRead {
    Value(U256),
    /// Not answerable at this block. `from` is where this key's coverage starts, if anywhere.
    NotCovered { from: Option<u64> },
}

pub fn encode_key(k: &StateKey) -> [u8; K] {
    let mut out = [0u8; K];
    let (tag, a, slot): (u8, &Address, Option<&U256>) = match k {
        StateKey::Balance(a) => (1, a, None),
        StateKey::Nonce(a) => (2, a, None),
        StateKey::CodeHash(a) => (3, a, None),
        StateKey::Storage(a, s) => (4, a, Some(s)),
    };
    out[0] = tag;
    out[1..21].copy_from_slice(a.as_slice());
    if let Some(s) = slot {
        out[21..].copy_from_slice(&s.to_be_bytes::<32>());
    }
    out
}

pub fn decode_key(b: &[u8; K]) -> Option<StateKey> {
    let a = Address::from_slice(&b[1..21]);
    Some(match b[0] {
        1 => StateKey::Balance(a),
        2 => StateKey::Nonce(a),
        3 => StateKey::CodeHash(a),
        4 => StateKey::Storage(a, U256::from_be_slice(&b[21..])),
        _ => return None,
    })
}

fn vkey(k: &[u8; K], block: u64) -> [u8; VK] {
    let mut out = [0u8; VK];
    out[..K].copy_from_slice(k);
    out[K..].copy_from_slice(&block.to_be_bytes());
    out
}

/// One processed block for the archive.
pub struct ArchiveBlock<'a> {
    pub number: u64,
    pub hash: B256,
    /// RLP-encoded header (execution environment for historical calls).
    pub header_rlp: &'a [u8],
    /// Final writes of this block to archived keys.
    pub writes: &'a [(StateKey, U256)],
}

pub struct Archive {
    db: Database,
}

impl Archive {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ArchiveError> {
        let db = Database::create(path)?;
        let tx = db.begin_write()?;
        tx.open_table(VERSIONS)?;
        tx.open_table(COVERAGE)?;
        tx.open_table(ARCHIVED)?;
        tx.open_table(HEADERS)?;
        tx.open_table(HASHES)?;
        tx.open_table(BY_HASH)?;
        tx.open_table(CODE)?;
        tx.open_table(JOURNAL)?;
        tx.open_table(SEGMENTS)?;
        tx.commit()?;
        Ok(Self { db })
    }

    pub fn archived_keys(&self) -> Result<HashSet<StateKey>, ArchiveError> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(ARCHIVED)?;
        let mut out = HashSet::new();
        for e in t.iter()? {
            let (k, _) = e?;
            if let Some(k) = decode_key(k.value()) {
                out.insert(k);
            }
        }
        Ok(out)
    }

    /// Marks keys as archived (idempotent). Their history starts at their first base or write.
    pub fn add_keys(&self, keys: &[StateKey]) -> Result<(), ArchiveError> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(ARCHIVED)?;
            for k in keys {
                t.insert(&encode_key(k), ())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Last block of the open segment.
    pub fn last_block(&self) -> Result<Option<u64>, ArchiveError> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(SEGMENTS)?;
        Ok(t.last()?.map(|(_, end)| end.value()))
    }

    pub fn segments(&self) -> Result<Vec<(u64, u64)>, ArchiveError> {
        let tx = self.db.begin_read()?;
        let t = tx.open_table(SEGMENTS)?;
        let mut out = Vec::new();
        for e in t.iter()? {
            let (s, e) = e?;
            out.push((s.value(), e.value()));
        }
        Ok(out)
    }

    /// Records a processed block. Consecutive blocks extend the open segment; anything else
    /// starts a new one (the gap stays uncovered).
    pub fn write_block(&self, b: &ArchiveBlock<'_>) -> Result<(), ArchiveError> {
        let tx = self.db.begin_write()?;
        {
            let mut seg = tx.open_table(SEGMENTS)?;
            let last = seg.last()?.map(|(s, e)| (s.value(), e.value()));
            match last {
                Some((start, end)) if end + 1 == b.number => {
                    seg.insert(start, b.number)?;
                }
                Some((_, end)) if b.number <= end => return Err(ArchiveError::NotNext(b.number, Some(end))),
                _ => {
                    seg.insert(b.number, b.number)?;
                }
            }
            Self::write_entries(&tx, b)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn write_entries(tx: &redb::WriteTransaction, b: &ArchiveBlock<'_>) -> Result<(), ArchiveError> {
        let mut versions = tx.open_table(VERSIONS)?;
        let mut cov = tx.open_table(COVERAGE)?;
        let mut journal = Vec::with_capacity(b.writes.len() * K);
        for (k, v) in b.writes {
            let ek = encode_key(k);
            versions.insert(&vkey(&ek, b.number), &v.to_be_bytes::<32>())?;
            // Coverage starts at the earliest block whose value is known (a gap fill can move it
            // earlier: a write fixes the value from that block on).
            let cur = cov.get(&ek)?.map(|g| g.value());
            if cur.is_none_or(|c| b.number < c) {
                cov.insert(&ek, b.number)?;
            }
            journal.extend_from_slice(&ek);
        }
        tx.open_table(JOURNAL)?.insert(b.number, journal.as_slice())?;
        tx.open_table(HEADERS)?.insert(b.number, b.header_rlp)?;
        tx.open_table(HASHES)?.insert(b.number, &b.hash.0)?;
        tx.open_table(BY_HASH)?.insert(&b.hash.0, b.number)?;
        Ok(())
    }

    /// Fills the gap between two segments with the missing blocks (in order) and merges them.
    /// `blocks` must start right after a segment's end, end right before the next segment's
    /// start, and link by hash to both: `first_parent` = hash at the previous end, and the last
    /// block's hash = parent of the next segment's first header (checked by `next_parent`).
    pub fn fill_gap(&self, blocks: &[ArchiveBlock<'_>], first_parent: B256, next_parent: B256) -> Result<(), ArchiveError> {
        let (Some(first), Some(last)) = (blocks.first(), blocks.last()) else { return Ok(()) };
        let segs = self.segments()?;
        let prev = segs.iter().find(|(_, e)| *e + 1 == first.number).copied();
        let next = segs.iter().find(|(s, _)| *s == last.number + 1).copied();
        let (Some((ps, _)), Some((ns, ne))) = (prev, next) else {
            return Err(ArchiveError::GapMismatch(format!("{}..={} is not between two segments", first.number, last.number)));
        };
        if self.hash(first.number - 1)? != Some(first_parent) {
            return Err(ArchiveError::GapMismatch("first block does not link to the previous segment".into()));
        }
        if last.hash != next_parent {
            return Err(ArchiveError::GapMismatch("last block does not link to the next segment".into()));
        }
        if blocks.windows(2).any(|w| w[1].number != w[0].number + 1) {
            return Err(ArchiveError::GapMismatch("blocks are not consecutive".into()));
        }
        let tx = self.db.begin_write()?;
        for b in blocks {
            Self::write_entries(&tx, b)?;
        }
        {
            let mut seg = tx.open_table(SEGMENTS)?;
            seg.remove(ns)?;
            seg.insert(ps, ne)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// A key's value became known at `block` (e.g. a verified SNAP base), plus any writes to it
    /// in later, already-archived blocks. Coverage starts at the earliest such block.
    pub fn write_base(&self, key: &StateKey, block: u64, value: U256, later: &[(u64, U256)]) -> Result<(), ArchiveError> {
        let ek = encode_key(key);
        let tx = self.db.begin_write()?;
        {
            let mut versions = tx.open_table(VERSIONS)?;
            // A BAL write at `block` is authoritative; don't overwrite it with the base.
            if versions.get(&vkey(&ek, block))?.is_none() {
                versions.insert(&vkey(&ek, block), &value.to_be_bytes::<32>())?;
            }
            for (n, v) in later {
                versions.insert(&vkey(&ek, *n), &v.to_be_bytes::<32>())?;
            }
            let mut cov = tx.open_table(COVERAGE)?;
            let cur = cov.get(&ek)?.map(|g| g.value());
            if cur.is_none_or(|c| block < c) {
                cov.insert(&ek, block)?;
            }
            tx.open_table(ARCHIVED)?.insert(&ek, ())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn put_code(&self, hash: B256, code: &[u8]) -> Result<(), ArchiveError> {
        let tx = self.db.begin_write()?;
        tx.open_table(CODE)?.insert(&hash.0, code)?;
        tx.commit()?;
        Ok(())
    }

    pub fn code(&self, hash: &B256) -> Result<Option<Bytes>, ArchiveError> {
        let tx = self.db.begin_read()?;
        Ok(tx.open_table(CODE)?.get(&hash.0)?.map(|g| Bytes::copy_from_slice(g.value())))
    }

    pub fn header_rlp(&self, number: u64) -> Result<Option<Vec<u8>>, ArchiveError> {
        let tx = self.db.begin_read()?;
        Ok(tx.open_table(HEADERS)?.get(number)?.map(|g| g.value().to_vec()))
    }

    pub fn hash(&self, number: u64) -> Result<Option<B256>, ArchiveError> {
        let tx = self.db.begin_read()?;
        Ok(tx.open_table(HASHES)?.get(number)?.map(|g| B256::from(*g.value())))
    }

    pub fn number_of(&self, hash: &B256) -> Result<Option<u64>, ArchiveError> {
        let tx = self.db.begin_read()?;
        Ok(tx.open_table(BY_HASH)?.get(&hash.0)?.map(|g| g.value()))
    }

    /// Reorg: forget blocks above `n` (their versions, headers, journal) and end the segment at
    /// `n`. Coverage that started in a forgotten block is withdrawn.
    pub fn truncate_above(&self, n: u64) -> Result<(), ArchiveError> {
        let tx = self.db.begin_write()?;
        {
            let mut journal = tx.open_table(JOURNAL)?;
            let mut versions = tx.open_table(VERSIONS)?;
            let mut cov = tx.open_table(COVERAGE)?;
            let mut headers = tx.open_table(HEADERS)?;
            let mut hashes = tx.open_table(HASHES)?;
            let mut by_hash = tx.open_table(BY_HASH)?;
            let blocks: Vec<(u64, Vec<u8>)> = journal
                .range((n + 1)..)?
                .map(|e| e.map(|(b, j)| (b.value(), j.value().to_vec())))
                .collect::<Result<_, _>>()?;
            for (b, j) in &blocks {
                for ek in j.chunks_exact(K) {
                    let ek: &[u8; K] = ek.try_into().unwrap();
                    versions.remove(&vkey(ek, *b))?;
                    if cov.get(ek)?.is_some_and(|c| c.value() == *b) {
                        cov.remove(ek)?;
                    }
                }
                journal.remove(*b)?;
                headers.remove(*b)?;
                if let Some(h) = hashes.remove(*b)? {
                    by_hash.remove(h.value())?;
                }
            }
            let mut seg = tx.open_table(SEGMENTS)?;
            let open: Vec<(u64, u64)> = seg.iter()?.map(|e| e.map(|(s, e)| (s.value(), e.value()))).collect::<Result<_, _>>()?;
            for (s, e) in open {
                if s > n {
                    seg.remove(s)?;
                } else if e > n {
                    seg.insert(s, n)?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The value of `key` at block `number`, if the archive covers it.
    pub fn get(&self, key: &StateKey, number: u64) -> Result<ArchiveRead, ArchiveError> {
        let ek = encode_key(key);
        let tx = self.db.begin_read()?;
        let from = tx.open_table(COVERAGE)?.get(&ek)?.map(|g| g.value());
        let in_segment = {
            let seg = tx.open_table(SEGMENTS)?;
            let r = seg.range(..=number)?.next_back().transpose()?;
            r.map(|(s, e)| (s.value(), e.value()))
        };
        let Some((seg_start, seg_end)) = in_segment.filter(|(_, e)| number <= *e) else {
            return Ok(ArchiveRead::NotCovered { from });
        };
        let _ = seg_end;
        // The newest version at or below `number`, and it must lie within this segment.
        let v = tx.open_table(VERSIONS)?;
        let (lo, hi) = (vkey(&ek, 0), vkey(&ek, number));
        let found = v.range::<&[u8; VK]>(&lo..=&hi)?.next_back().transpose()?;
        match (from, found) {
            (Some(c), Some((k, val))) if c <= number => {
                let at = u64::from_be_bytes(k.value()[K..].try_into().unwrap());
                if at < seg_start {
                    // The last known version predates a gap: writes in the gap are unknown.
                    return Ok(ArchiveRead::NotCovered { from });
                }
                Ok(ArchiveRead::Value(U256::from_be_bytes(*val.value())))
            }
            _ => Ok(ArchiveRead::NotCovered { from }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: Address = Address::repeat_byte(0xaa);

    fn slot(i: u64) -> StateKey {
        StateKey::Storage(A, U256::from(i))
    }

    fn blk(a: &Archive, n: u64, writes: &[(StateKey, u64)]) {
        let w: Vec<_> = writes.iter().map(|(k, v)| (*k, U256::from(*v))).collect();
        a.write_block(&ArchiveBlock { number: n, hash: B256::with_last_byte(n as u8), header_rlp: &[0xc0], writes: &w }).unwrap();
    }

    fn val(a: &Archive, k: StateKey, n: u64) -> Option<u64> {
        match a.get(&k, n).unwrap() {
            ArchiveRead::Value(v) => Some(v.to()),
            ArchiveRead::NotCovered { .. } => None,
        }
    }

    #[test]
    fn stores_only_changes_and_answers_any_covered_block() {
        let d = tempfile::tempdir().unwrap();
        let a = Archive::open(d.path().join("a.redb")).unwrap();
        blk(&a, 100, &[]);
        a.write_base(&slot(1), 100, U256::from(10), &[]).unwrap();
        blk(&a, 101, &[]);
        blk(&a, 102, &[(slot(1), 20)]);
        blk(&a, 103, &[]);
        assert_eq!(val(&a, slot(1), 99), None); // before coverage
        assert_eq!(val(&a, slot(1), 100), Some(10));
        assert_eq!(val(&a, slot(1), 101), Some(10));
        assert_eq!(val(&a, slot(1), 102), Some(20));
        assert_eq!(val(&a, slot(1), 103), Some(20));
        assert_eq!(val(&a, slot(1), 104), None); // beyond the archive
        assert_eq!(val(&a, slot(2), 102), None); // never archived
    }

    #[test]
    fn late_discovered_key_is_not_backdated() {
        let d = tempfile::tempdir().unwrap();
        let a = Archive::open(d.path().join("a.redb")).unwrap();
        for n in 1..=5 {
            blk(&a, n, &[]);
        }
        a.write_base(&slot(3), 4, U256::from(7), &[]).unwrap();
        assert_eq!(val(&a, slot(3), 3), None);
        assert!(matches!(a.get(&slot(3), 3).unwrap(), ArchiveRead::NotCovered { from: Some(4) }));
        assert_eq!(val(&a, slot(3), 5), Some(7));
    }

    #[test]
    fn reorg_truncates_exactly_the_orphaned_writes() {
        let d = tempfile::tempdir().unwrap();
        let a = Archive::open(d.path().join("a.redb")).unwrap();
        blk(&a, 1, &[]);
        a.write_base(&slot(1), 1, U256::from(1), &[]).unwrap();
        blk(&a, 2, &[(slot(1), 2)]);
        blk(&a, 3, &[(slot(1), 3), (slot(9), 9)]);
        a.truncate_above(2).unwrap();
        assert_eq!(val(&a, slot(1), 2), Some(2));
        assert_eq!(val(&a, slot(1), 3), None); // block 3 forgotten
        assert_eq!(a.last_block().unwrap(), Some(2));
        assert!(matches!(a.get(&slot(9), 2).unwrap(), ArchiveRead::NotCovered { from: None }));
        blk(&a, 3, &[(slot(1), 33)]); // the new branch
        assert_eq!(val(&a, slot(1), 3), Some(33));
    }

    #[test]
    fn a_late_gap_fill_merges_segments_and_restores_continuity() {
        let d = tempfile::tempdir().unwrap();
        let a = Archive::open(d.path().join("a.redb")).unwrap();
        blk(&a, 10, &[]);
        a.write_base(&slot(1), 10, U256::from(5), &[]).unwrap();
        blk(&a, 11, &[]);
        blk(&a, 15, &[]); // restart: 12..14 missing
        a.write_base(&slot(1), 15, U256::from(8), &[]).unwrap();
        assert_eq!(val(&a, slot(1), 13), None);
        let w13 = [(slot(1), U256::from(8))];
        let gap = [
            ArchiveBlock { number: 12, hash: B256::with_last_byte(12), header_rlp: &[0xc0], writes: &[] },
            ArchiveBlock { number: 13, hash: B256::with_last_byte(13), header_rlp: &[0xc0], writes: &w13 },
            ArchiveBlock { number: 14, hash: B256::with_last_byte(14), header_rlp: &[0xc0], writes: &[] },
        ];
        // Must link on both sides.
        assert!(a.fill_gap(&gap, B256::with_last_byte(99), B256::with_last_byte(14)).is_err());
        a.fill_gap(&gap, B256::with_last_byte(11), B256::with_last_byte(14)).unwrap();
        assert_eq!(a.segments().unwrap(), vec![(10, 15)]);
        assert_eq!(val(&a, slot(1), 12), Some(5));
        assert_eq!(val(&a, slot(1), 13), Some(8));
        assert_eq!(val(&a, slot(1), 15), Some(8));
    }

    #[test]
    fn gaps_split_segments_and_persist_across_reopen() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("a.redb");
        {
            let a = Archive::open(&path).unwrap();
            blk(&a, 10, &[]);
            a.write_base(&slot(1), 10, U256::from(5), &[]).unwrap();
            blk(&a, 11, &[]);
        }
        let a = Archive::open(&path).unwrap(); // restart
        assert_eq!(a.last_block().unwrap(), Some(11));
        blk(&a, 15, &[]); // restarted without filling 12..14
        assert_eq!(a.segments().unwrap(), vec![(10, 11), (15, 15)]);
        assert_eq!(val(&a, slot(1), 11), Some(5));
        assert_eq!(val(&a, slot(1), 13), None); // inside the gap
        assert_eq!(val(&a, slot(1), 15), None); // writes in the gap are unknown
        a.write_base(&slot(1), 15, U256::from(6), &[]).unwrap(); // refetched after restart
        assert_eq!(val(&a, slot(1), 15), Some(6));
        assert!(a.archived_keys().unwrap().contains(&slot(1)));
    }
}
