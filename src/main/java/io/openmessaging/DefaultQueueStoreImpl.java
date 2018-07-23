package io.openmessaging;


import io.openmessaging.io.AsyncIO;
import io.openmessaging.io.AsyncIO_task;
import io.openmessaging.utils.MessageB64Serialization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {
    static String file_prefix = "/alidata1/race2018/data/data";
    static long TOTAL_QUEUE_NUM = 1000000;
    public static int IO_THREAD = 4;
    static int CLUSTER_SIZE = 5;
    public static int SUBMIT_BATCH_SIZE = 4096;
    static int queue_nums[] = new int[IO_THREAD];
    static int cluster_size[] = new int[IO_THREAD];

    static FileChannel data_file_handles[] = new FileChannel[IO_THREAD];
    static FileChannel index_file_handles[] = new FileChannel[IO_THREAD];

    AsyncIO asyncIO;

    public static ConcurrentHashMap<Long, AsyncIO_task[]> taskMap = new ConcurrentHashMap<>();

    static long getQueueID(String queueName) {
        long res = 0;
        long multiplier = 1;
        for (int i = queueName.length() - 1; i >= 0 && queueName.charAt(i) >= '0' && queueName.charAt(i) <= '9'; i--) {
            res += (queueName.charAt(i) - '0') * multiplier;
            multiplier *= 10;
        }
        return res;
    }

    public DefaultQueueStoreImpl() {
        int q_ave = (int) TOTAL_QUEUE_NUM / IO_THREAD;
        for (int i = 0; i < IO_THREAD; i++) {
            queue_nums[i] = q_ave;
            cluster_size[i] = CLUSTER_SIZE;
        }
        queue_nums[IO_THREAD - 1] = (int) TOTAL_QUEUE_NUM - q_ave * (IO_THREAD - 1);
        cluster_size[IO_THREAD - 1] = CLUSTER_SIZE;

        asyncIO = new AsyncIO(file_prefix, IO_THREAD, queue_nums, cluster_size);
        asyncIO.start();

        for (int i = 0; i < IO_THREAD; i++) {
            data_file_handles[i] = asyncIO.work_threads[i].data_file_fd;
            index_file_handles[i] = asyncIO.work_threads[i].index_file_fd;
        }
    }

    public void put(String queueName, byte[] message) {
        long tid = Thread.currentThread().getId();
        if (!taskMap.containsKey(tid)) {
            AsyncIO_task[] tasks = new AsyncIO_task[IO_THREAD];
            for (int i = 0; i < IO_THREAD; i++) {
                tasks[i] = new AsyncIO_task(SUBMIT_BATCH_SIZE);
            }
            taskMap.put(tid, tasks);
        }
        long queueID = getQueueID(queueName);
        int threadID = (int) (queueID % IO_THREAD);
        AsyncIO_task[] taskBuffers = taskMap.get(tid);
        AsyncIO_task taskBuffer = taskBuffers[threadID];

        taskBuffer.put(queueID, message);
        if (taskBuffer.write_counter == SUBMIT_BATCH_SIZE) {
            asyncIO.submitIOTask(threadID, taskBuffer);
            taskBuffers[threadID] = new AsyncIO_task(SUBMIT_BATCH_SIZE);
        }
    }

    public Collection<byte[]> get(String queueName, long offset, long num) {
        asyncIO.waitFinishIO(taskMap);
        Collection<byte[]> results = new ArrayList<>((int) num);
        long queueID = getQueueID(queueName);

        int threadID = (int) (queueID % IO_THREAD);
        int batch_size = asyncIO.work_threads[threadID].batch_size;
        int queue_num = asyncIO.work_threads[threadID].queue_num_per_file;
        int which_queue_in_this_io_thread = (int) (queueID / IO_THREAD);

        long queue_count = asyncIO.work_threads[threadID].queue_counter[which_queue_in_this_io_thread];
        long max_offset = Math.min(offset + num, queue_count);

        ByteBuffer index_record = ByteBuffer.allocate(AsyncIO.INDEX_ENTRY_SIZE * batch_size);
        for (long queue_offset = offset; queue_offset < max_offset; ) {
            long left_num = batch_size - (queue_offset % batch_size);
            if (queue_offset + left_num > max_offset) {
                left_num = max_offset - queue_offset;
            }
            long chunk_id = ((queue_offset / batch_size) * (queue_num * batch_size) +
                    (which_queue_in_this_io_thread * batch_size) + queue_offset % batch_size);
            long idx_file_offset = AsyncIO.INDEX_ENTRY_SIZE * chunk_id;

            index_record.position(0);
            index_record.limit((int) (left_num * AsyncIO.INDEX_ENTRY_SIZE));
            try {
                asyncIO.work_threads[(int) (queueID % IO_THREAD)].index_file_fd.read(index_record, idx_file_offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (int element = 0; element < left_num; element++) {
                byte output_buf[];
                if (index_record.get(AsyncIO.INDEX_ENTRY_SIZE * (element + 1) - 1) == AsyncIO.LARGE_MESSAGE_MAGIC_CHAR) {
                    long large_msg_size;
                    long large_msg_offset;
                    large_msg_offset = index_record.getLong(4 + AsyncIO.INDEX_ENTRY_SIZE * element);
                    large_msg_size = index_record.getLong(12 + AsyncIO.INDEX_ENTRY_SIZE * element);
                    ByteBuffer large_msg_buf = ByteBuffer.allocate((int) large_msg_size);
                    try {
                        asyncIO.work_threads[(int) (queueID % IO_THREAD)].data_file_fd.read(large_msg_buf, large_msg_offset);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    output_buf = MessageB64Serialization.DeserializeBase64EncodingAddIndex(large_msg_buf.array(),
                            0, large_msg_buf.position(), (int) (queue_offset + element));
                } else {

                    output_buf = MessageB64Serialization.DeserializeBase64EncodingAddIndex(index_record.array(),
                            element * AsyncIO.INDEX_ENTRY_SIZE, AsyncIO.INDEX_ENTRY_SIZE, (int) (queue_offset + element));
                }
                results.add(output_buf);
            }
            queue_offset += left_num;
        }
        return results;

    }
}
