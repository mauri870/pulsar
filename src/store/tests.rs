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
    assert_eq!(normalized(store.into_groups().unwrap()), expected());
}

#[test]
fn spill_store_stays_in_memory_below_the_limit() {
    let directory = unique_directory("below-limit");
    let mut store = SpillStore::new(directory.clone(), usize::MAX);
    fill(&mut store);
    assert_eq!(store.key_count(), 3);
    assert!(!directory.exists(), "no database should have been created");
    assert_eq!(normalized(store.into_groups().unwrap()), expected());
}

#[test]
fn spill_store_merges_groups_across_spills() {
    let directory = unique_directory("every-batch");
    // A limit of one byte spills after every batch.
    let mut store = SpillStore::new(directory.clone(), 1);
    fill(&mut store);
    assert!(store.database.is_some(), "the store should have spilled");
    assert_eq!(store.key_count(), 3);
    assert_eq!(normalized(store.into_groups().unwrap()), expected());
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
        normalized(store.into_groups().unwrap()),
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
    store.into_groups().unwrap();
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
