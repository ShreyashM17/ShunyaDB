use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global seqno counter (in-memory)
/// Starts at 0 internally; first issued seqno is 1
static GLOBAL_SEQNO: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Allocate the next sequence number (monotonic, unique).
/// Return the new seqno (value >= 1).
pub fn allocate() -> u64 {
  let prev = GLOBAL_SEQNO.fetch_add(1, Ordering::SeqCst);
  prev + 1
}

pub fn current() -> u64 {
  GLOBAL_SEQNO.load(Ordering::SeqCst)
}

/// Ensure global seqno is at least `min`. If current < min, advance it to `min`.
/// Returns the resulting current seqno after the operation (>= min).
pub fn advance_to(min: u64) -> u64 {
  let cur = GLOBAL_SEQNO.load(Ordering::SeqCst);
  if cur >= min {
    return cur;
  }
  let _ = GLOBAL_SEQNO.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
    if old >= min {
      None
    } else {
      Some(min)
    }
  });

  GLOBAL_SEQNO.load(Ordering::SeqCst)
}


#[cfg(test)]
mod tests;