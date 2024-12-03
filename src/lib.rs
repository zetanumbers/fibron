use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    future::{Future, IntoFuture},
    io,
    mem::transmute,
    num::NonZero,
    pin::pin,
    ptr,
    sync::{mpsc, Arc, Mutex},
    task::{self, Poll},
    thread,
};

use corosensei::{fiber, Fiber};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use futures_lite::future::block_on;

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
            .map(|thrd_idx| {
                let worker = Worker::new_lifo();
                let (tx, rx) = mpsc::channel();
                (
                    ThreadInfo {
                        stealer: worker.stealer(),
                    },
                    WorkerThread {
                        thrd_idx,
                        lifo_queue: worker,
                        free_fibers: RefCell::new(Vec::new()),
                        fiber_queue_out: rx,
                        fiber_queue_in: tx,
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
                        move || {
                            WorkerThread::set_current(&worker);
                            main_loop(&r, &worker)
                        }
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
fn main_loop(registry: &Registry, local: &WorkerThread) -> ! {
    loop {
        match local.fiber_queue_out.try_recv() {
            Ok(rx) => match rx.try_recv() {
                Ok(fiber) => {
                    let local = ptr::addr_of!(*local);
                    let job =
                        fiber.switch(move |f| unsafe { (*local).free_fibers.borrow_mut().push(f) });
                    if let Some(job) = job {
                        job();
                    }
                    continue;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    panic!("fiber queue is expected to contain receivers with received value")
                }
                // Fiber waker is dropped without waking, thus consider this fiber to be stuck
                Err(mpsc::TryRecvError::Disconnected) => continue,
            },
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => panic!("fiber queue is closed"),
        }

        let steal = local
            .lifo_queue
            .pop()
            .or_else(|| registry.injector.steal().success())
            .or_else(|| {
                registry
                    .threads
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != local.thrd_idx)
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

        // oneshot async channel
        let (tx_b, rx_b) = flume::bounded(1);

        // FIXME: use `Job` trait from rayon
        // SAFETY: we do not exit the scope until `task_b` executes
        // FIXME: handle unwinding
        let task_b = unsafe {
            transmute::<Box<dyn FnOnce() + Send + '_>, Box<dyn FnOnce() + Send>>(Box::new(|| {
                tx_b.try_send(oper_b()).unwrap();
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
            match rx_b.try_recv() {
                Ok(b) => break b,
                Err(flume::TryRecvError::Disconnected) => todo!("endless task"),
                Err(flume::TryRecvError::Empty) => (),
            }

            let mut task = worker.lifo_queue.pop();
            if let Some(t) = task.take() {
                if ptr::eq(&*t, addr_b) {
                    t();
                    continue;
                } else {
                    task = Some(t);
                }
            }

            let mut rx_b = pin!(rx_b.into_recv_async());
            let (tx, rx) = mpsc::sync_channel(1);
            let waker = Arc::new(FiberWaker {
                tx: worker.fiber_queue_in.clone(),
                rx: Mutex::new(Some(rx)),
            })
            .into();
            let mut cx = task::Context::from_waker(&waker);

            break loop {
                let task = task.take();

                if let Poll::Ready(res_b) = rx_b.as_mut().poll(&mut cx) {
                    match res_b {
                        Ok(b) => break b,
                        Err(flume::RecvError::Disconnected) => todo!("endless task"),
                    }
                }

                let tx = tx.clone();
                let free_fiber = worker.free_fibers.borrow_mut().pop();
                let worker = ptr::addr_of!(*worker);
                match free_fiber {
                    Some(f) => f.switch(move |f| {
                        tx.try_send(f).unwrap();
                        task
                    }),
                    None => {
                        let registry = Arc::clone(&self.registry);

                        fiber::<Infallible>().switch(move |f| {
                            tx.try_send(f).unwrap();
                            if let Some(task) = task {
                                task();
                            }
                            main_loop(&registry, unsafe { &*worker })
                        });
                    }
                };
            };
        };

        (res_a, res_b)
    }

    pub fn wait_for<F>(&self, fut: F) -> F::Output
    where
        F: IntoFuture,
    {
        let fut = fut.into_future();
        let worker = WorkerThread::current();
        if worker.is_null() {
            return block_on(fut);
        }
        let worker = unsafe { &*worker };

        let mut fut = pin!(fut);
        let (tx, rx) = mpsc::sync_channel(1);
        let waker = Arc::new(FiberWaker {
            tx: worker.fiber_queue_in.clone(),
            rx: Mutex::new(Some(rx)),
        })
        .into();
        let mut cx = task::Context::from_waker(&waker);

        loop {
            // TODO: allow to poll on other fibers
            if let Poll::Ready(res_b) = fut.as_mut().poll(&mut cx) {
                return res_b;
            }

            let tx = tx.clone();
            let free_fiber = worker.free_fibers.borrow_mut().pop();
            match free_fiber {
                Some(f) => f.switch(move |f| {
                    tx.try_send(f).unwrap();
                    None
                }),
                None => {
                    let registry = Arc::clone(&self.registry);
                    let worker = ptr::addr_of!(*worker);

                    fiber::<Infallible>().switch(move |f| {
                        tx.try_send(f).unwrap();
                        main_loop(&registry, unsafe { &*worker })
                    });
                }
            };
        }
    }
}

struct ThreadInfo {
    stealer: Stealer<Job>,
}

struct WorkerThread {
    thrd_idx: usize,
    lifo_queue: Worker<Job>,
    free_fibers: RefCell<Vec<Fiber<Option<Job>>>>,
    fiber_queue_out: mpsc::Receiver<mpsc::Receiver<Fiber<()>>>,
    fiber_queue_in: mpsc::Sender<mpsc::Receiver<Fiber<()>>>,
}

unsafe impl Send for WorkerThread {}

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

struct FiberWaker {
    tx: mpsc::Sender<mpsc::Receiver<Fiber<()>>>,
    rx: Mutex<Option<mpsc::Receiver<Fiber<()>>>>,
}

unsafe impl Sync for FiberWaker {}
unsafe impl Send for FiberWaker {}

impl task::Wake for FiberWaker {
    fn wake(self: Arc<Self>) {
        let Ok(mut rx) = self.rx.lock() else {
            return;
        };
        let Some(rx) = rx.take() else {
            return;
        };
        let _ = self.tx.send(rx);
    }
}
