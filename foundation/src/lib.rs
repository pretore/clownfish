use std::sync::Once;

use crate::executors::Executioner;

/// Checks for passed in arguments in functions.
pub mod common;
/// Handling of configuration (re)loading.
pub mod configuration;
/// Reporting of measurements which are turned into metrics.
pub mod metrics;
/// Parsing of
/// [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier)s.
pub mod uri;

mod executors;

fn executor() -> &'static Executioner {
    static INIT: Once = Once::new();
    static mut EXECUTIONER: Option<Executioner> = None;
    unsafe {
        INIT.call_once(|| {
            EXECUTIONER = Some(Executioner::new());
        });
        EXECUTIONER.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::executor;

    #[test]
    fn executor_successfully_used() {
        let sum = Arc::new(AtomicUsize::new(0));
        // retrieve an executor and get its id
        let id = executor().id();
        for i in 1..11 {
            let clone = sum.clone();
            let e = executor();
            // ensure that the executors are the same
            assert_eq!(id, e.id());
            e.submit(move || {
                clone.fetch_add(i, Ordering::SeqCst);
            });
        }
        // We sleep here to give the newly created threads time to complete their work
        thread::sleep(Duration::from_secs(1));
        // Please see wikipedia link for the formula used below of [n+(n+1)]/2
        // https://en.wikipedia.org/wiki/1_%2B_2_%2B_3_%2B_4_%2B_%E2%8B%AF#Partial_sums
        assert_eq!((10 * 11) / 2, sum.load(Ordering::SeqCst));
    }
}
