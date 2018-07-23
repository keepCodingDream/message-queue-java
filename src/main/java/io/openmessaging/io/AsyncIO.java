package io.openmessaging.io;

import io.openmessaging.DefaultQueueStoreImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

enum IO_thread_status {INITING, RUNNING, CLOSING};

public class AsyncIO {
    public static byte LARGE_MESSAGE_MAGIC_CHAR = '!';
    public static int INDEX_ENTRY_SIZE = 43;
    static int MAX_BLOCKING_IO_TASK = 64;
    static long FILE_EXTEND_SIZE = 64 * 1024 * 1024;
    static int MAX_INDEX_MAPPED_BLOCK_NUM = 8;
    static int FORCE_REMAP_NUM = 1;
    static long INDEX_MAPPED_BLOCK_SIZE = (INDEX_ENTRY_SIZE * 1024 * 512);
    static long INDEX_BLOCK_WRITE_TIMES_TO_FULL = (INDEX_MAPPED_BLOCK_SIZE / INDEX_ENTRY_SIZE);

    static boolean finished = false;
    static AtomicInteger finished_thread = new AtomicInteger(0);
    static boolean sended_flush_msg = false;
    static Lock send_lock = new ReentrantLock();
    static int thread_num;
    public IOThread work_threads[];

    public AsyncIO(String file_prefix, int thread_num, int queue_num_per_file[], int batch_size[]) {
        AsyncIO.thread_num = thread_num;
        this.work_threads = new IOThread[thread_num];
        for (int i = 0; i < thread_num; i++) {
            work_threads[i] = new IOThread(i, file_prefix, queue_num_per_file[i], batch_size[i], MAX_BLOCKING_IO_TASK);
        }
    }

    public void start() {
        for (int i = 0; i < thread_num; i++) {
            this.work_threads[i].start();
        }
    }

    public void submitIOTask(int which_thread, AsyncIO_task task) {
        if (work_threads[which_thread].status == IO_thread_status.RUNNING) {
            try {
                work_threads[which_thread].blockingQueue.put(task);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            System.out.printf("failed to submit IO task to %d thread\n", which_thread);
        }
    }

    public void waitFinishIO(ConcurrentHashMap<Long, AsyncIO_task[]> taskMap ) {
        if (!finished) {
//            DefaultQueueStoreImpl.tidSet.add(Thread.currentThread().getId());
//            System.out.println(Thread.currentThread().getId());
            send_lock.lock();
            if (!sended_flush_msg) {
                sended_flush_msg = true;
                try {
                    for (AsyncIO_task[] tasks : taskMap.values()) {
                        for (int i = 0; i < thread_num; i++) {
                            work_threads[i].blockingQueue.put(tasks[i]);
                        }
                    }
                    for (int i = 0; i < thread_num; i++) {
//                        if (work_threads[i].task.write_counter > 0) {
//                            work_threads[i].blockingQueue.put(work_threads[i].task);
//                        }
                        AsyncIO_task task = new AsyncIO_task();
                        task.write_counter = -1;

                        work_threads[i].blockingQueue.put(task);

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            send_lock.unlock();
            while (AsyncIO.finished_thread.get() < thread_num) ;
            finished = true;
        }
    }
}
