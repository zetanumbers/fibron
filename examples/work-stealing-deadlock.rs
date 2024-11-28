use std::{sync::Arc, thread::sleep, time::Duration};

fn main() {
    let thread_pool = Arc::new(
        fibron::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap(),
    );

    let (tx1, rx1) = flume::unbounded();
    let (tx2, rx2) = flume::unbounded();
    thread_pool.spawn({
        let thread_pool = Arc::clone(&thread_pool);
        move || {
            println!("task 0 START, thread_id: {:?}", std::thread::current());
            thread_pool.join(
                || {
                    println!("task 1 START, thread_id: {:?}", std::thread::current());
                    sleep(Duration::from_secs(1));
                    thread_pool.spawn(move || {
                        println!("task 3 START, thread_id: {:?}", std::thread::current());
                        rx1.recv().unwrap();
                        println!("task 3 DONE");
                    });
                    println!("task 1 DONE");
                },
                || {
                    println!("task 2 START, thread_id: {:?}", std::thread::current());
                    sleep(Duration::from_secs(2));
                    println!("task 2 DONE");
                },
            );
            println!("join DONE");
            tx2.send(()).unwrap();
            tx1.send(()).unwrap();
            println!("task 0 DONE");
        }
    });

    rx2.recv().unwrap();
}
