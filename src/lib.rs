use std::{
    cell::Cell,
    io,
    mem::transmute,
    num::NonZero,
    ptr,
    sync::{mpsc, Arc, Mutex},
    task::{self, Waker},
    thread,
};

use corosensei::Fiber;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};

const DEFAULT_STACK_SIZE: usize = 1024 * 1024;
const DEFAULT_FALLBACK_NUM_THREADS: usize = 4;

type Job = Box<dyn FnOnce() + Send>;

pub struct ThreadPoolBuilder {
    get_thread_name: Option<Box<dyn FnMut(usize) -> String>>,
    acquire_thread_handler: Option<Box<dyn Fn() + Send + Sync>>,
    release_thread_handler: Option<Box<dyn Fn() + Send + Sync>>,
    num_threads: Option<NonZero<usize>>,
    deadlock_handler: Option<Box<dyn Fn() + Send + Sync>>,
    stack_size: usize,
}

impl ThreadPoolBuilder {
    pub fn new() -> Self {
        ThreadPoolBuilder {
            get_thread_name: None,
            acquire_thread_handler: None,
            release_thread_handler: None,
            num_threads: None,
            deadlock_handler: None,
            stack_size: DEFAULT_STACK_SIZE,
        }
    }

    pub fn thread_name<F>(mut self, closure: F) -> Self
    where
        F: FnMut(usize) -> String + 'static,
    {
        self.get_thread_name = Some(Box::new(closure));
        self
    }

    pub fn acquire_thread_handler<H>(mut self, handler: H) -> Self
    where
        H: Fn() + Send + Sync + 'static,
    {
        self.acquire_thread_handler = Some(Box::new(handler));
        self
    }

    pub fn release_thread_handler<H>(mut self, handler: H) -> Self
    where
        H: Fn() + Send + Sync + 'static,
    {
        self.release_thread_handler = Some(Box::new(handler));
        self
    }

    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = NonZero::new(num_threads);
        self
    }

    pub fn deadlock_handler<H>(mut self, handler: H) -> Self
    where
        H: Fn() + Send + Sync + 'static,
    {
        self.deadlock_handler = Some(Box::new(handler));
        self
    }

    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = stack_size;
        self
    }

    pub fn build(self) -> io::Result<ThreadPool> {
        let num_threads = self
            .num_threads
            .or_else(|| thread::available_parallelism().ok())
            .map(NonZero::get)
            .unwrap_or(DEFAULT_FALLBACK_NUM_THREADS);
        let (threads, workers): (Vec<_>, Vec<_>) = (0..num_threads)
            .map(|_| {
                let worker = Worker::new_lifo();
                let (tx, rx) = mpsc::channel
                (
                    ThreadInfo {
                        stealer: worker.stealer(),
                    },
                    WorkerThread {
                        lifo_queue: worker,
                        current_fiber_waker: todo!(),
                        free_fibers: Vec::new(),
                        fiber_in_queue: todo!(),
                        fiber_out_queue: todo!(),
                    },
                )
            })
            .unzip();
        let registry = Arc::new(Registry {
            threads,
            injector: Injector::new(),
        });
        workers
            .into_iter()
            .enumerate()
            .try_for_each(|(i, worker)| {
                // TODO: simply warn if unable to create enough threads?
                if let Err(err) = thread::Builder::new()
                    .stack_size(self.stack_size)
                    .name(format!("fibron_thread_{i}"))
                    .spawn({
                        let r = Arc::clone(&registry);
                        move || main_loop(i, &r, &worker)
                    })
                {
                    // FIXME: terminate threads
                    return Err(err);
                };
                Ok(())
            })?;
        Ok(ThreadPool { registry })
    }
}

// FIXME: use fibers
fn main_loop(thrd_idx: usize, registry: &Registry, local: &WorkerThread) -> ! {
    WorkerThread::set_current(local);
    loop {
        let steal = local
            .lifo_queue
            .pop()
            .or_else(|| registry.injector.steal().success())
            .or_else(|| {
                registry
                    .threads
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != thrd_idx)
                    .find_map(|(_, thread)| loop {
                        match thread.stealer.steal() {
                            Steal::Empty => break None,
                            Steal::Success(job) => break Some(job),
                            Steal::Retry => continue,
                        }
                    })
            });
        match steal {
            // FIXME: wait for jobs
            None => std::thread::yield_now(),
            Some(job) => job(),
        }
        // FIXME: termination
    }
}

pub struct ThreadPool {
    registry: Arc<Registry>,
}

struct Registry {
    threads: Vec<ThreadInfo>,
    injector: Injector<Job>,
}

impl ThreadPool {
    pub fn spawn<OP>(&self, op: OP)
    where
        OP: FnOnce() + Send + 'static,
    {
        let worker = WorkerThread::current();
        if worker.is_null() {
            self.registry.injector.push(Box::new(op))
        } else {
            (unsafe { &*worker }).lifo_queue.push(Box::new(op))
        }
    }

    pub fn join<A, B, RA, RB>(&self, oper_a: A, oper_b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let worker = WorkerThread::current();
        if worker.is_null() {
            let (txa, rxa) = mpsc::sync_channel(1);
            let (txb, rxb) = mpsc::sync_channel(1);
            // SAFETY: TODO
            let task_a = unsafe {
                transmute::<Box<dyn FnOnce() + Send + '_>, Box<dyn FnOnce() + Send>>(Box::new(
                    || {
                        txa.try_send(oper_a()).unwrap();
                    },
                ))
            };
            let task_b = unsafe {
                transmute::<Box<dyn FnOnce() + Send + '_>, Box<dyn FnOnce() + Send>>(Box::new(
                    || {
                        txb.try_send(oper_b()).unwrap();
                    },
                ))
            };
            self.registry.injector.push(task_a);
            self.registry.injector.push(task_b);
            return (rxa.recv().unwrap(), rxb.recv().unwrap());
        }
        let worker = unsafe { &*worker };

        let (tx, rx) = mpsc::sync_channel(1);
        // FIXME: use `Job` trait from rayon
        // SAFETY: we do not exit the scope until `task_b` executes
        // FIXME: handle unwinding
        let task_b = unsafe {
            transmute::<Box<dyn FnOnce() + Send + '_>, Box<dyn FnOnce() + Send>>(Box::new(|| {
                tx.try_send(oper_b()).unwrap();
            }))
        };
        // NOTE: assuming box address doesn't change
        let addr_b = &*task_b as *const _;
        worker.lifo_queue.push(task_b);
        // FIXME: handle unwinding
        let res_a = oper_a();

        // let mut is_task_inaccessable = false;
        // let mut other_tasks_stack = Vec::new();
        let res_b = loop {
            match rx.try_recv() {
                Ok(b) => break b,
                Err(mpsc::TryRecvError::Disconnected) => unimplemented!(),
                Err(mpsc::TryRecvError::Empty) => (),
            }

            // if !is_task_inaccessable {
            if let Some(task) = worker.lifo_queue.pop() {
                if ptr::eq(&*task, addr_b) {
                task();
                } else {
                //     other_tasks_stack.push(task);
                }
            } else {
                // is_task_inaccessable = true
            }
            // }
        };

        // for other_task in other_tasks_stack {
        //     worker.lifo_queue.push(other_task);
        // }

        (res_a, res_b)
    }
}

struct ThreadInfo {
    stealer: Stealer<Job>,
}

impl ThreadInfo {
    fn new(stealer: Stealer<Job>) -> Self {
        Self { stealer }
    }
}

struct WorkerThread {
    lifo_queue: Worker<Job>,
    current_fiber_waker: Waker,
    free_fibers: Vec<Fiber<Option<Job>>>,
    fiber_in_queue: mpsc::Receiver<mpsc::Receiver<Fiber<()>>>,
    fiber_out_queue: mpsc::Sender<Fiber<()>>,
}

impl WorkerThread {
    fn current() -> *const WorkerThread {
        WORKER_THREAD_STATE.get()
    }

    fn set_current(worker: *const WorkerThread) {
        WORKER_THREAD_STATE.with(|worker_ptr| {
            debug_assert!(worker_ptr.get().is_null());
            worker_ptr.set(worker);
        });
    }
}

thread_local! {
    static WORKER_THREAD_STATE: Cell<*const WorkerThread> = const { Cell::new(ptr::null()) };
}

// TODO: proper waker implementation
struct FiberWakerState {
    tx: mpsc::Sender<mpsc::Receiver<Fiber<()>>>,
    rx: Option<mpsc::Receiver<Fiber<()>>>,
}

struct FiberWaker {
    state: Mutex<FiberWakerState>,
}

impl FiberWaker {
    fn new(
        tx: mpsc::Sender<mpsc::Receiver<Fiber<()>>>,
        rx: mpsc::Receiver<Fiber<()>>,
    ) -> Self {
        Self {
            state: Mutex::new(FiberWakerState { tx, rx: Some(rx) }),
        }
    }
}

impl task::Wake for FiberWaker {
    fn wake(self: Arc<Self>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(rx) = state.rx.take() else {
            return;
        };
        let _ = state.tx.send(rx);
    }
}
