use std::num::NonZero;

pub struct ThreadPoolBuilder {
    get_thread_name: Option<Box<dyn FnMut(usize) -> String>>,
    acquire_thread_handler: Option<Box<dyn Fn() + Send + Sync>>,
    release_thread_handler: Option<Box<dyn Fn() + Send + Sync>>,
    num_threads: Option<NonZero<usize>>,
    deadlock_handler: Option<Box<dyn Fn() + Send + Sync>>,
    stack_size: Option<usize>,
}

impl ThreadPoolBuilder {
    pub fn new() -> Self {
        ThreadPoolBuilder {
            get_thread_name: None,
            acquire_thread_handler: None,
            release_thread_handler: None,
            num_threads: None,
            deadlock_handler: None,
            stack_size: None,
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
        self.stack_size = Some(stack_size);
        self
    }
}
