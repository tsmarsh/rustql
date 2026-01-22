use std::sync::atomic::{AtomicBool, Ordering};

static SHARED_CACHE_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn set_shared_cache_enabled(enabled: bool) {
    SHARED_CACHE_ENABLED.store(enabled, Ordering::SeqCst);
}

pub fn shared_cache_enabled() -> bool {
    SHARED_CACHE_ENABLED.load(Ordering::SeqCst)
}
