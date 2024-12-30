use std::{
    future::Future,
    sync::mpsc::{sync_channel,Receiver,SyncSender},
    sync::{Arc,Mutex},
    task::Context,
    time::Duration,
};

use timer_future::TimerFuture;

use futures::{task::{waker_ref,ArcWake}, future::{FutureExt, BoxFuture}};

struct Executor{
    ready_queue:Receiver<Arc<Task>>,
}

impl Executor{
    fn run(&self){
        while let Ok(task)=self.ready_queue.recv(){
            let mut future_slot = task.futuer.lock().unwrap();
            if let Some(mut future) = future_slot.take(){
                let waker = waker_ref(&task);
                let context = &mut Context::from_waker(&*waker);
                if future.as_mut().poll(context).is_pending(){
                    *future_slot = Some(future);
                }
            }
        }
    }
}

#[derive(Clone)]
struct Spawner{
    task_sender:SyncSender<Arc<Task>>,
}

impl Spawner{
    fn spawn(&self,future:impl Future<Output=()>+'static+Send){
        let future = future.boxed();
        let task = Arc::new(Task{
            futuer:Mutex::new(Some(future)),
            task_sender:self.task_sender.clone(),
        });
        self.task_sender.send(task).expect("队列已满");
    }
}

struct Task{
    futuer:Mutex<Option<BoxFuture<'static,()>>>,
    task_sender:SyncSender<Arc<Task>>,
}

impl ArcWake for Task{
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let cloned = arc_self.clone();
        arc_self.task_sender.send(cloned).expect("队列已满");
    }
}

fn new_executor_and_spawner()->(Executor,Spawner){
    const MAX_QUEUE_TASKS:usize = 10_000;
    let (task_sender,ready_queue) = sync_channel(MAX_QUEUE_TASKS);
    (Executor{ready_queue},Spawner{task_sender})
}

fn main(){
    let (executor,spawner) = new_executor_and_spawner();

    spawner.spawn(async{
        println!("howdy!");
        TimerFuture::new(Duration::new(2,0)).await;
        println!("done!");
    });

    drop(spawner);

    executor.run();
}