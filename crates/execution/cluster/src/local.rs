//! Thread-local scope that keeps sharded-table reads local.

use std::cell::Cell;

thread_local! {
    static LOCAL_READS: Cell<u32> = const { Cell::new(0) };
}

/// Scope in which reads of the sharded state tables made on the current thread stay local and
/// see only the rows this node holds.
///
/// Trie computation walks ordered ranges and normally seeks into and past ranges the node does
/// not hold, so it must neither go to a peer nor fail there. Scopes nest; the read mode ends when
/// the outermost guard is dropped.
#[derive(Debug)]
pub struct LocalReads {
    _not_send: std::marker::PhantomData<*const ()>,
}

impl LocalReads {
    /// Enters the scope on the current thread.
    pub fn enter() -> Self {
        LOCAL_READS.with(|depth| depth.set(depth.get() + 1));
        Self { _not_send: std::marker::PhantomData }
    }

    /// Whether the current thread is inside a scope.
    pub fn is_active() -> bool {
        LOCAL_READS.with(|depth| depth.get() > 0)
    }
}

impl Drop for LocalReads {
    fn drop(&mut self) {
        LOCAL_READS.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

#[cfg(test)]
mod tests {
    use super::LocalReads;

    #[test]
    fn scope_is_active_until_the_outermost_guard_drops() {
        assert!(!LocalReads::is_active());
        let outer = LocalReads::enter();
        let inner = LocalReads::enter();
        drop(inner);
        assert!(LocalReads::is_active());
        drop(outer);
        assert!(!LocalReads::is_active());
    }

    #[test]
    fn scope_does_not_leak_to_other_threads() {
        let _guard = LocalReads::enter();
        assert!(!std::thread::spawn(LocalReads::is_active).join().unwrap());
    }
}
