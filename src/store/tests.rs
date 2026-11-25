use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A directory name that no other test (or process) uses.
pub(crate) fn unique_directory(name: &str) -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "pulsar-store-{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Every group of the store, in batches of two, flattened.
fn all_groups(store: impl GroupStore) -> Vec<(String, Vec<Value>)> {
    store
        .into_batches(2)
        .collect::<Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .flatten()
        .collect()
}

fn pairs(items: &[(&str, i64)]) -> Vec<KeyValue> {
    items
        .iter()
        .map(|(key, number)| KeyValue {
            key: key.to_string(),
            value: Value::Int(*number),
        })
        .collect()
}

/// The groups with keys and values in a fixed order, for comparison.
fn normalized(groups: Vec<(String, Vec<Value>)>) -> Vec<(String, Vec<i64>)> {
    let mut groups: Vec<(String, Vec<i64>)> = groups
        .into_iter()
        .map(|(key, values)| {
            let mut numbers: Vec<i64> = values
                .into_iter()
                .map(|value| match value {
                    Value::Int(number) => number,
                    other => panic!("unexpected value {:?}", other),
                })
                .collect();
            numbers.sort();
            (key, numbers)
        })
        .collect();
    groups.sort();
    groups
}

fn expected() -> Vec<(String, Vec<i64>)> {
    vec![
        ("a".to_string(), vec![1, 3, 5]),
        ("b".to_string(), vec![2, 6]),
        ("c".to_string(), vec![4]),
    ]
}

fn fill(store: &mut impl GroupStore) {
    store.add(pairs(&[("a", 1), ("b", 2)])).unwrap();
    store.add(pairs(&[("a", 3), ("c", 4)])).unwrap();
    store.add(pairs(&[("a", 5), ("b", 6)])).unwrap();
    store.finish().unwrap();
}

#[test]
fn memory_store_groups_values_by_key() {
    let mut store = MemoryStore::default();
    fill(&mut store);
    assert_eq!(store.key_count(), 3);
    assert_eq!(normalized(all_groups(store)), expected());
}

#[test]
fn spill_store_stays_in_memory_below_the_limit() {
    let directory = unique_directory("below-limit");
    let mut store = SpillStore::new(directory.clone(), usize::MAX);
    fill(&mut store);
    assert_eq!(store.key_count(), 3);
    assert!(!directory.exists(), "no database should have been created");
    assert_eq!(normalized(all_groups(store)), expected());
}

#[test]
fn spill_store_merges_groups_across_spills() {
    let directory = unique_directory("every-batch");
    // A limit of one byte spills after every batch.
    let mut store = SpillStore::new(directory.clone(), 1);
    fill(&mut store);
    assert!(store.database.is_some(), "the store should have spilled");
    assert_eq!(store.key_count(), 3);
    assert_eq!(normalized(all_groups(store)), expected());
}

#[test]
fn spill_store_flushes_what_is_left_in_memory_when_finished() {
    let directory = unique_directory("final-flush");
    // The first batch (four new groups) reaches the limit and spills; the
    // second (one new group) stays in memory until `finish`.
    let one_group = new_group_bytes("a") + Value::Int(1).estimated_size();
    let mut store = SpillStore::new(directory.clone(), one_group * 3 + 1);
    store.add(pairs(&[("a", 1), ("b", 2), ("c", 3), ("d", 4)])).unwrap();
    assert!(store.database.is_some(), "the first batch should have spilled");
    store.add(pairs(&[("a", 5)])).unwrap();
    assert_eq!(store.memory.len(), 1, "the second batch should stay in memory");
    store.finish().unwrap();
    assert!(store.memory.is_empty());
    assert_eq!(store.key_count(), 4);
    assert_eq!(
        normalized(all_groups(store)),
        vec![
            ("a".to_string(), vec![1, 5]),
            ("b".to_string(), vec![2]),
            ("c".to_string(), vec![3]),
            ("d".to_string(), vec![4]),
        ]
    );
}

#[test]
fn spill_store_removes_its_directory() {
    let directory = unique_directory("cleanup");
    let mut store = SpillStore::new(directory.clone(), 1);
    fill(&mut store);
    assert!(directory.exists());
    all_groups(store);
    assert!(!directory.exists(), "the temporary database should be removed");
}

#[test]
fn spill_store_counts_the_size_of_values() {
    let directory = unique_directory("large-values");
    // One group, but a value far larger than the limit.
    let mut store = SpillStore::new(directory, 1000);
    let large = Value::String("x".repeat(10_000));
    store
        .add(vec![KeyValue { key: "k".to_string(), value: large }])
        .unwrap();
    assert!(store.database.is_some(), "a single large value should spill");
    assert_eq!(store.memory_bytes, 0);
}

#[test]
fn spill_store_adds_to_an_existing_group_without_counting_the_key_again() {
    let directory = unique_directory("same-key");
    let mut store = SpillStore::new(directory, usize::MAX);
    store.add(pairs(&[("a", 1)])).unwrap();
    let after_first = store.memory_bytes;
    store.add(pairs(&[("a", 2)])).unwrap();
    assert_eq!(store.memory_bytes - after_first, Value::Int(2).estimated_size());
}

fn batch_sizes(store: impl GroupStore, batch_size: usize) -> Vec<usize> {
    store
        .into_batches(batch_size)
        .map(|batch| batch.unwrap().len())
        .collect()
}

fn many_groups(store: &mut impl GroupStore, count: usize) {
    let items: Vec<(String, i64)> = (0..count).map(|n| (format!("key{}", n), n as i64)).collect();
    let refs: Vec<(&str, i64)> = items.iter().map(|(key, n)| (key.as_str(), *n)).collect();
    store.add(pairs(&refs)).unwrap();
    store.finish().unwrap();
}

#[test]
fn memory_store_hands_out_groups_in_batches() {
    let mut store = MemoryStore::default();
    many_groups(&mut store, 7);
    assert_eq!(batch_sizes(store, 3), [3, 3, 1]);
}

#[test]
fn spill_store_hands_out_groups_in_batches_from_memory_and_from_disk() {
    let mut in_memory = SpillStore::new(unique_directory("batches-memory"), usize::MAX);
    many_groups(&mut in_memory, 7);
    assert_eq!(batch_sizes(in_memory, 3), [3, 3, 1]);

    let mut on_disk = SpillStore::new(unique_directory("batches-disk"), 1);
    many_groups(&mut on_disk, 7);
    assert!(on_disk.database.is_some());
    assert_eq!(batch_sizes(on_disk, 3), [3, 3, 1]);
}

#[test]
fn an_empty_store_has_no_batches() {
    assert!(batch_sizes(MemoryStore::default(), 4).is_empty());
    assert!(batch_sizes(SpillStore::new(unique_directory("empty"), 1), 4).is_empty());
}

#[test]
fn spill_store_keeps_its_database_until_the_batches_are_consumed() {
    let directory = unique_directory("lazy");
    let mut store = SpillStore::new(directory.clone(), 1);
    many_groups(&mut store, 5);
    let mut batches = store.into_batches(2);
    assert!(directory.exists(), "the database must outlive into_batches");
    assert_eq!(batches.next().unwrap().unwrap().len(), 2);
    assert!(directory.exists());
    drop(batches);
    assert!(!directory.exists(), "the database is removed with the iterator");
}
