use std::{fmt, thread};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Condvar, Mutex, TryLockError};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use crate::common::{CommonError, required};

static COUNTER: AtomicU8 = AtomicU8::new(0);

#[derive(Debug)]
pub struct Executioner {
    id: u8,
    workers: Mutex<Vec<Mutex<Worker>>>,
}

impl Executioner {
    pub fn new() -> Self {
        Executioner {
            id: COUNTER.fetch_add(1, Ordering::SeqCst),
            workers: Mutex::new(vec![]),
        }
    }

    pub fn id(
        &self
    ) -> u8 {
        self.id
    }

    pub fn submit<F>(
        &self,
        callable: F,
    )
        where
            F: FnOnce() + Send + 'static,
    {
        let mut vec = self.workers.lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut expired = Vec::new();
        for i in 0..vec.len() {
            let mutex = match vec.get_mut(i) {
                None => continue,
                Some(m) => m
            };
            let worker = match mutex.lock() {
                Ok(w) => w,
                Err(_) => {
                    expired.push(i);
                    continue;
                }
            };
            let mut data = match worker.mutex.try_lock() {
                Ok(d) => d,
                Err(e) => {
                    match e {
                        TryLockError::Poisoned(_) => {
                            expired.push(i);
                        }
                        _ => {}
                    }
                    continue;
                }
            };
            if !worker.running.load(Ordering::SeqCst) {
                expired.push(i);
                continue;
            }
            if data.is_none() {
                *data = Some(Box::new(callable));
                worker.condvar.notify_one();
                return;
            }
        }
        for i in expired {
            vec.remove(i);
        }
        let mut next_id: usize = 0;
        for i in 0..vec.len() {
            let mutex = match vec.get(i) {
                None => continue,
                Some(m) => m
            };
            let worker = match mutex.lock() {
                Ok(w) => w,
                Err(_) => {
                    continue;
                }
            };
            if next_id <= worker.id {
                next_id = 1 + worker.id;
            }
        }
        let name = format!("{}:{}", self.id, next_id);
        let worker = match Worker::new(next_id, &name, callable) {
            Ok(w) => w,
            Err(_) => {
                // We have a programmatic error, abort.
                panic!("Invalid name of '{}' given to worker", name);
            }
        };
        vec.push(Mutex::new(worker));
    }
}

struct Worker {
    id: usize,
    mutex: Arc<Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>>,
    condvar: Arc<Condvar>,
    running: Arc<AtomicBool>,
}

impl Worker {
    pub fn new<F>(
        id: usize,
        name: &str,
        callable: F,
    ) -> Result<Self, CommonError>
        where
            F: FnOnce() + Send + 'static,
    {
        required(name, "name")?;
        let w = Worker {
            id,
            mutex: Arc::new(Mutex::new(Some(Box::new(callable)))),
            condvar: Arc::new(Condvar::new()),
            running: Arc::new(AtomicBool::new(true)),
        };
        let mutex = w.mutex.clone();
        let condvar = w.condvar.clone();
        let running = w.running.clone();
        thread::Builder::new().name(name.to_string()).spawn(move || {
            while running.load(Ordering::SeqCst) {
                let mut result =
                    condvar.wait_timeout_while(
                        mutex.lock()
                            .unwrap_or_else(|e| e.into_inner()),
                        Duration::from_secs(60),
                        |callable| {
                            callable.is_none()
                        }).unwrap_or_else(|e| e.into_inner());
                // Remember that the mutex is locked at this point, so no modifications
                // are allowed to it outside this thread from this point
                if let Some(callable) = result.0.take() {
                    callable();
                } else {
                    running.store(false, Ordering::SeqCst);
                }
            }
        }).unwrap();
        Ok(w)
    }
}

impl Debug for Worker {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("running", &self.running)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::common::CommonError;
    use crate::foundation::executors::{Executioner, Worker};

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn worker_error_on_creating_with_empty_name() {
        let error = Worker::new(0, "", || {}).unwrap_err();
        match error {
            CommonError::Empty(message) => {
                assert_eq!(message, "name is empty")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn worker_error_on_creating_with_blank_name() {
        let error = Worker::new(0, "\t\n", || {}).unwrap_err();
        match error {
            CommonError::Invalid(message) => {
                assert_eq!(message, "name is blank")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn worker_successfully_create_and_use_until_expiry() {
        // Create a closure to call that will have side effects
        let callback_was_called = Arc::new(AtomicBool::new(false));
        let clone = callback_was_called.clone();
        // Create worker and assert that it is in a running state
        let worker = Worker::new(0, "test", move || {
            clone.store(true, Ordering::SeqCst);
        }).unwrap();
        assert!(worker.running.load(Ordering::SeqCst));
        println!("{:#?}", worker);
        // Wait until the worker has expired
        thread::sleep(Duration::from_secs(61));
        assert!(callback_was_called.load(Ordering::SeqCst));
        assert!(!worker.running.load(Ordering::SeqCst));
    }

    #[test]
    fn executioner_successfully_create_and_use() {
        let executioner = Executioner::new();
        assert_eq!(0, executioner.id);
        {
            let vec = executioner.workers.lock().unwrap();
            assert!(vec.is_empty());
        }
        let sum = Arc::new(AtomicUsize::new(0));
        // Sum first 1000 natural numbers by creating many individual requests of work
        for i in 1..1001 {
            let clone = sum.clone();
            executioner.submit(move || {
                clone.fetch_add(i, Ordering::SeqCst);
            });
        }
        // We sleep here to give the newly created threads time to complete their work
        thread::sleep(Duration::from_secs(1));
        // Please see wikipedia link for the formula used below of [n+(n+1)]/2
        // https://en.wikipedia.org/wiki/1_%2B_2_%2B_3_%2B_4_%2B_%E2%8B%AF#Partial_sums
        assert_eq!((1000 * 1001) / 2, sum.load(Ordering::SeqCst));
        {
            let vec = executioner.workers.lock().unwrap();
            println!("{}", format!("Executioner worker count: '{}'", vec.len()));
            // We expect a major reduction in the total number of workers created in respect to
            // the requested 1000 individual work items
            assert!(vec.len() < 200);
            println!("{:#?}", executioner);
        }
    }
}