//! Grouping of the mapped pairs by key. The pipeline only sees the
//! [`GroupStore`] trait; whether groups stay in memory or spill to disk is
//! up to the implementation.

use bincode::{deserialize_from, serialize_into};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::info;

use crate::error::{Error, Result};
use crate::value::{KeyValue, Value};

/// A key and every value that was mapped to it.
pub type Group = (String, Vec<Value>);

/// Collects pairs by key.
pub trait GroupStore: Send + 'static {
    /// Adds pairs to their groups.
    fn add(&mut self, pairs: Vec<KeyValue>) -> Result<()>;

    /// Called once after the last `add`.
    fn finish(&mut self) -> Result<()>;

    /// Number of distinct keys, or for a store that spilled, an upper bound
    /// (a key spilled more than once is counted each time). Meaningful after `finish`.
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

/// Files holding the groups a [`SpillStore`] moved out of memory, one file per
/// spill and each sorted by key. They are removed, along with their
/// directory, when this is dropped.
struct Runs {
    directory: PathBuf,
    /// Each file and the number of groups in it.
    files: Vec<(PathBuf, usize)>,
}

impl Runs {
    /// Writes `groups` to a new run file, sorted by key.
    fn write(&mut self, groups: HashMap<String, Vec<Value>>) -> Result<()> {
        std::fs::create_dir_all(&self.directory).map_err(Error::Spill)?;
        let path = self.directory.join(format!("run-{}.bin", self.files.len()));

        let mut entries: Vec<Group> = groups.into_iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut writer = BufWriter::new(File::create(&path).map_err(Error::Spill)?);
        for entry in &entries {
            serialize_into(&mut writer, entry)?;
        }
        writer.flush().map_err(Error::Spill)?;
        self.files.push((path, entries.len()));
        Ok(())
    }
}

impl Drop for Runs {
    fn drop(&mut self) {
        for (path, _) in &self.files {
            let _ = std::fs::remove_file(path);
        }
        if !self.files.is_empty() {
            let _ = std::fs::remove_dir(&self.directory);
        }
    }
}

/// Reads the groups of one run file in order.
struct RunReader {
    reader: BufReader<File>,
    /// The next group of the run, if any is left.
    current: Option<Group>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(Error::Spill)?;
        let mut reader = RunReader {
            reader: BufReader::new(file),
            current: None,
        };
        reader.advance()?;
        Ok(reader)
    }

    fn advance(&mut self) -> Result<()> {
        let at_end = self.reader.fill_buf().map_err(Error::Spill)?.is_empty();
        self.current = if at_end {
            None
        } else {
            Some(deserialize_from(&mut self.reader)?)
        };
        Ok(())
    }
}

/// Merges the sorted runs into groups in key order, combining the values of a
/// key that appears in several runs. The files are opened when the first
/// group is asked for, and are removed when the iterator is dropped. After an
/// error the iterator ends.
fn merge_runs(runs: Runs) -> impl Iterator<Item = Result<Group>> + Send {
    let mut readers: Option<Vec<RunReader>> = None;
    let mut failed = false;
    std::iter::from_fn(move || {
        if failed {
            return None;
        }
        let next = (|| -> Result<Option<Group>> {
            let readers = match &mut readers {
                Some(readers) => readers,
                None => {
                    let opened = runs
                        .files
                        .iter()
                        .map(|(path, _)| RunReader::open(path))
                        .collect::<Result<Vec<_>>>()?;
                    readers.insert(opened)
                }
            };

            let Some(key) = readers
                .iter()
                .filter_map(|reader| reader.current.as_ref().map(|(key, _)| key))
                .min()
                .cloned()
            else {
                return Ok(None);
            };

            let mut values = Vec::new();
            for reader in readers.iter_mut() {
                if matches!(&reader.current, Some((current, _)) if *current == key) {
                    if let Some((_, mut more)) = reader.current.take() {
                        values.append(&mut more);
                    }
                    reader.advance()?;
                }
            }
            Ok(Some((key, values)))
        })();
        match next {
            Ok(group) => group.map(Ok),
            Err(error) => {
                failed = true;
                Some(Err(error))
            }
        }
    })
}

/// Keeps groups in memory until they grow past a limit, then writes them to a
/// sorted run file in `directory` and starts over. Reading the groups back
/// merges the runs, so a key that was spilled several times still comes out
/// as one group.
pub struct SpillStore {
    memory: HashMap<String, Vec<Value>>,
    /// Estimated size of what `memory` holds.
    memory_bytes: usize,
    runs: Runs,
    memory_limit_bytes: usize,
}

impl SpillStore {
    pub fn new(directory: PathBuf, memory_limit_bytes: usize) -> Self {
        SpillStore {
            memory: HashMap::new(),
            memory_bytes: 0,
            runs: Runs {
                directory,
                files: Vec::new(),
            },
            memory_limit_bytes,
        }
    }

    /// Writes every in-memory group to a new run.
    fn spill(&mut self) -> Result<()> {
        info!("Flushing {} groups to disk", self.memory.len());
        // Taking the map also releases its capacity, which `drain` would keep.
        let groups = std::mem::take(&mut self.memory);
        self.runs.write(groups)?;
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
        if !self.runs.files.is_empty() && !self.memory.is_empty() {
            info!("Final flush of {} remaining groups to disk", self.memory.len());
            self.spill()?;
        }
        Ok(())
    }

    fn key_count(&self) -> usize {
        // A key that was spilled more than once is counted once per run.
        self.memory.len() + self.runs.files.iter().map(|(_, count)| count).sum::<usize>()
    }

    fn into_batches(self, batch_size: usize) -> impl Iterator<Item = Result<Vec<Group>>> + Send {
        let groups: Box<dyn Iterator<Item = Result<Group>> + Send> = if self.runs.files.is_empty() {
            Box::new(self.memory.into_iter().map(Ok))
        } else {
            Box::new(merge_runs(self.runs))
        };
        batches(groups, batch_size)
    }
}

#[cfg(test)]
pub(crate) mod tests;
