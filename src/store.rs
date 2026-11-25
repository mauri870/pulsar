//! Grouping of the mapped pairs by key. The pipeline only sees the
//! [`GroupStore`] trait; whether groups stay in memory or spill to disk is
//! up to the implementation.

use bincode::{deserialize, serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use tracing::info;

use crate::error::Result;
use crate::value::{KeyValue, Value};

/// Memory the groups may use before [`SpillStore`] moves them to disk.
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB

/// A key and every value that was mapped to it.
pub type Group = (String, Vec<Value>);

/// Collects pairs by key.
pub trait GroupStore: Send + 'static {
    /// Adds pairs to their groups.
    fn add(&mut self, pairs: Vec<KeyValue>) -> Result<()>;

    /// Called once after the last `add`.
    fn finish(&mut self) -> Result<()>;

    /// Number of distinct keys. Meaningful after `finish`.
    fn key_count(&self) -> usize;

    /// Every group, consuming the store, in batches of at most `batch_size`
    /// groups that are produced one at a time, so the groups need not all be
    /// in memory together. Meaningful after `finish`.
    fn into_batches(self, batch_size: usize) -> impl Iterator<Item = Result<Vec<Group>>> + Send;
}

/// Cuts a sequence of groups into batches. A failure ends the batch it
/// occurs in and is returned in its place.
fn batches(
    mut groups: impl Iterator<Item = Result<Group>> + Send,
    batch_size: usize,
) -> impl Iterator<Item = Result<Vec<Group>>> + Send {
    std::iter::from_fn(move || {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
            match groups.next() {
                Some(Ok(group)) => batch.push(group),
                Some(Err(error)) => return Some(Err(error)),
                None => break,
            }
        }
        if batch.is_empty() { None } else { Some(Ok(batch)) }
    })
}

/// Keeps every group in memory. Used by the tests; in the engine a
/// [`SpillStore`] behaves the same until its limit is reached.
#[cfg(test)]
#[derive(Default)]
pub struct MemoryStore {
    groups: HashMap<String, Vec<Value>>,
}

#[cfg(test)]
impl GroupStore for MemoryStore {
    fn add(&mut self, pairs: Vec<KeyValue>) -> Result<()> {
        for pair in pairs {
            self.groups.entry(pair.key).or_default().push(pair.value);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn key_count(&self) -> usize {
        self.groups.len()
    }

    fn into_batches(self, batch_size: usize) -> impl Iterator<Item = Result<Vec<Group>>> + Send {
        batches(self.groups.into_iter().map(Ok), batch_size)
    }
}

/// Memory a group takes before its first value: the map entry and the key.
fn new_group_bytes(key: &str) -> usize {
    std::mem::size_of::<(String, Vec<Value>)>() + key.len()
}

fn merge_values(_key: &[u8], old_value: Option<&[u8]>, new_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut values: Vec<Value> = old_value
        .and_then(|bytes| deserialize(bytes).ok())
        .unwrap_or_default();
    let new_values: Vec<Value> = deserialize(new_bytes).unwrap_or_default();
    values.extend(new_values);
    serialize(&values).ok()
}

/// Keeps groups in memory until they grow past a limit, then moves them to a
/// temporary on-disk database in `directory` and keeps merging into it.
pub struct SpillStore {
    memory: HashMap<String, Vec<Value>>,
    database: Option<sled::Db>,
    /// Estimated size of what `memory` holds.
    memory_bytes: usize,
    directory: PathBuf,
    memory_limit_bytes: usize,
}

impl SpillStore {
    pub fn new(directory: PathBuf, memory_limit_bytes: usize) -> Self {
        SpillStore {
            memory: HashMap::new(),
            database: None,
            memory_bytes: 0,
            directory,
            memory_limit_bytes,
        }
    }

    /// Moves every in-memory group into the database, creating it first if needed.
    fn spill(&mut self) -> Result<()> {
        if self.database.is_none() {
            let database = sled::Config::default()
                .path(&self.directory)
                .temporary(true)
                .open()?;
            database.set_merge_operator(merge_values);
            self.database = Some(database);
        }
        let database = self.database.as_ref().unwrap();

        info!("Flushing {} entries to DB", self.memory.len());
        // Taking the map also releases its capacity, which `drain` would keep.
        for (key, values) in std::mem::take(&mut self.memory) {
            database.merge(key.as_bytes(), serialize(&values)?)?;
        }
        self.memory_bytes = 0;
        Ok(())
    }
}

impl GroupStore for SpillStore {
    fn add(&mut self, pairs: Vec<KeyValue>) -> Result<()> {
        for pair in pairs {
            self.memory_bytes += pair.value.estimated_size();
            match self.memory.entry(pair.key) {
                Entry::Occupied(mut group) => group.get_mut().push(pair.value),
                Entry::Vacant(group) => {
                    self.memory_bytes += new_group_bytes(group.key());
                    group.insert(vec![pair.value]);
                }
            }
        }
        if self.memory_bytes >= self.memory_limit_bytes {
            self.spill()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.database.is_some() && !self.memory.is_empty() {
            info!("Final flush of {} remaining entries to DB", self.memory.len());
            self.spill()?;
        }
        Ok(())
    }

    fn key_count(&self) -> usize {
        match &self.database {
            Some(database) => database.len(),
            None => self.memory.len(),
        }
    }

    fn into_batches(self, batch_size: usize) -> impl Iterator<Item = Result<Vec<Group>>> + Send {
        let groups: Box<dyn Iterator<Item = Result<Group>> + Send> = match self.database {
            None => Box::new(self.memory.into_iter().map(Ok)),
            Some(database) => {
                let entries = database.iter();
                Box::new(entries.map(move |entry| -> Result<Group> {
                    // The temporary database is removed when it is dropped,
                    // so it must live as long as the iterator.
                    let _database = &database;
                    let (key, value_bytes) = entry?;
                    let key = String::from_utf8_lossy(&key).to_string();
                    let values: Vec<Value> = deserialize(&value_bytes)?;
                    Ok((key, values))
                }))
            }
        };
        batches(groups, batch_size)
    }
}

#[cfg(test)]
pub(crate) mod tests;
