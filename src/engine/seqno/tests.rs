use super::*;
use serial_test::serial;
use std::collections::HashSet;
use std::thread;

// Test-only helper to reset global seqno for clean test runs.
// This is only compiled in test builds.
fn reset_for_tests() {
  GLOBAL_SEQNO.store(0, Ordering::SeqCst);
}

#[test]
#[serial]
fn allocate_increments() {
  reset_for_tests();
  let a = allocate();
  let b = allocate();
  let c = allocate();
  assert_eq!(a + 1, b);
  assert_eq!(b + 1, c);
  assert_eq!(current(), c);
  assert!(a >= 1);
}

#[test]
#[serial]
fn advance_to_sets_value_when_lower() {
  reset_for_tests();
  assert_eq!(current(), 0);
  let after = advance_to(100);
  assert_eq!(after, 100);
  assert_eq!(current(), 100);
  // Allocate should return 101 next
  let next = allocate();
  assert_eq!(next, 101);
}

#[test]
#[serial]
fn advance_to_noop_when_higher() {
  reset_for_tests();
  // allocate a few
  let _ = allocate(); // 1
  let _ = allocate(); // 2
  let cur = current();
  let after = advance_to(cur - 1); // smaller than current
  assert_eq!(after, cur);
}

#[test]
#[serial]
fn concurrent_allocations_are_unique_and_monotonic() {
  reset_for_tests();
  let threads = 16;
  let per_thread = 5_000; // total 80k allocations
  let mut handles = Vec::with_capacity(threads);

  for _ in 0..threads {
    handles.push(thread::spawn(move || {
    let mut local = Vec::with_capacity(per_thread);
    for _ in 0..per_thread {
      local.push(crate::engine::seqno::allocate());
    }
    local
    }));
  }

  // collect all seqnos
  let mut all = Vec::with_capacity(threads * per_thread);
  for h in handles {
    let v = h.join().expect("thread join");
    all.extend(v);
  }

  assert_eq!(all.len(), threads * per_thread);

  // check uniqueness
  let set: HashSet<u64> = all.iter().copied().collect();
  assert_eq!(set.len(), all.len(), "seqnos must be unique");

  // check range continuity: since we reset to 0, we expect sequence 1..=N
  let mut all_sorted = all;
  all_sorted.sort_unstable();
  let n = threads * per_thread;
  for (i, val) in all_sorted.iter().enumerate() {
    let expected = (i as u64) + 1;
    assert_eq!(*val, expected, "expected {}, got {}", expected, val);
  }

  // current should be n
  assert_eq!(current(), n as u64);
}