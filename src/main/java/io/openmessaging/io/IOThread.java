package io.openmessaging.io;

import io.openmessaging.DefaultQueueStoreImpl;
import io.openmessaging.utils.MessageB64Serialization;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import static io.openmessaging.utils.UnsafeUtils.absolutePut;
import static io.openmessaging.utils.UnsafeUtils.unmap;

public class IOThread extends Thread {

    private void doIO(AsyncIO_task task) {
        for (int i = 0; i < task.write_counter; i++) {
            byte[] message = task.data[i];
            int queue_offsst_idx = message.length - 8;
            long queue_offset = (message[queue_offsst_idx] & 0xff) + ((message[queue_offsst_idx + 1] & 0xff) << 8)
                    + ((message[queue_offsst_idx + 2] & 0xff) << 16);

            byte[] buf = MessageB64Serialization.SerializeBase64DecodingSkipIndex(message, message.length);
            int write_size = buf.length;

            ByteBuffer index_record = ByteBuffer.wrap(buf);
            if (write_size > AsyncIO.INDEX_ENTRY_SIZE) {
                try {
                    while (this.data_file_offset + write_size > this.data_file_size) {

                        this.data_file_fd.truncate(this.data_file_size + AsyncIO.FILE_EXTEND_SIZE);

                        this.data_file_size += AsyncIO.FILE_EXTEND_SIZE;
                    }
                    long offset_in_data_file = this.data_file_offset;
                    this.data_file_fd.write(index_record, offset_in_data_file);
                    this.data_file_offset += write_size;

                    index_record = ByteBuffer.allocate(AsyncIO.INDEX_ENTRY_SIZE);
                    //System.out.printf("write size %d\n", write_size);
                    index_record.putLong(4, offset_in_data_file);
                    index_record.putLong(12, write_size);
                    index_record.position(AsyncIO.INDEX_ENTRY_SIZE - 1);
                    index_record.put(AsyncIO.LARGE_MESSAGE_MAGIC_CHAR);
                    index_record.flip();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            long which_queue_in_this_io_thread = task.which_queues[i] / AsyncIO.thread_num;
            this.queue_counter[(int) which_queue_in_this_io_thread]++;

            long chunk_id = ((queue_offset / batch_size) * (queue_num_per_file * batch_size) +
                    (which_queue_in_this_io_thread * batch_size) + queue_offset % batch_size);

            long idx_file_offset = AsyncIO.INDEX_ENTRY_SIZE * chunk_id;
            try {
                write_to_idx(index_record, idx_file_offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void remmap_index_block() throws IOException {
        if (this.current_index_mapped_end_offset == 0) {//first time, need to map all index blocks
            this.current_index_mapped_start_offset = 0;
            this.current_index_mapped_end_offset = AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM * AsyncIO.INDEX_MAPPED_BLOCK_SIZE;
            this.index_file_size = this.current_index_mapped_end_offset;

            this.index_file_fd.truncate(this.index_file_size);
            for (int i = 0; i < AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM; i++) {
                this.index_file_mapped_blocks[i] = this.index_file_fd.map(FileChannel.MapMode.READ_WRITE,
                        i * AsyncIO.INDEX_MAPPED_BLOCK_SIZE, AsyncIO.INDEX_MAPPED_BLOCK_SIZE);
                this.index_mapped_block_write_counter[i] = 0;
            }

            return;
        }
        int remmap_counter = AsyncIO.FORCE_REMAP_NUM;//we remap at least 1 chunk, a.k.a the lest-most one in mapped array
        for (int i = 0; i < remmap_counter; i++) {
            unmap(this.index_file_mapped_blocks[i]);
        }
        for (int i = remmap_counter; i < AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM; i++) {
            if (this.index_mapped_block_write_counter[i] >= AsyncIO.INDEX_BLOCK_WRITE_TIMES_TO_FULL) {
                remmap_counter++;
                unmap(this.index_file_mapped_blocks[i]);
            } else {
                break;
            }
        }
//        System.out.printf("remap %d chunks\n", remmap_counter);
        //start to map new chunks
        this.current_index_mapped_start_offset += remmap_counter * AsyncIO.INDEX_MAPPED_BLOCK_SIZE;
        this.current_index_mapped_end_offset += remmap_counter * AsyncIO.INDEX_MAPPED_BLOCK_SIZE;
        this.index_file_size = this.current_index_mapped_end_offset;

        this.index_file_fd.truncate(this.index_file_size);

        for (int i = 0; i < AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM - remmap_counter; i++) {
            this.index_file_mapped_blocks[i] = this.index_file_mapped_blocks[i + remmap_counter];
        }
        for (int i = AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM - remmap_counter; i < AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM; i++) {
            this.index_file_mapped_blocks[i] = this.index_file_fd.map(FileChannel.MapMode.READ_WRITE,
                    i * AsyncIO.INDEX_MAPPED_BLOCK_SIZE + this.current_index_mapped_start_offset, AsyncIO.INDEX_MAPPED_BLOCK_SIZE);
            this.index_mapped_block_write_counter[i] = 0;
        }
    }

    private void write_to_idx(ByteBuffer index_record, long global_offset) throws IOException {
        if (global_offset < this.current_index_mapped_start_offset) {
            this.index_file_fd.write(index_record, global_offset);
            this.index_pwrite_times++;
            return;
        }
        while (global_offset >= this.current_index_mapped_end_offset) {
            remmap_index_block();
        }

        long offset_from_first_mapped_block = global_offset - this.current_index_mapped_start_offset;
        long which_mapped_chunk = (offset_from_first_mapped_block / AsyncIO.INDEX_MAPPED_BLOCK_SIZE);
        long offset_in_mapped_chunk = offset_from_first_mapped_block % AsyncIO.INDEX_MAPPED_BLOCK_SIZE;

        this.index_mapped_block_write_counter[(int) which_mapped_chunk]++;

        //option 1: use hacked absolutePut method
        absolutePut(this.index_file_mapped_blocks[(int) which_mapped_chunk], (int) offset_in_mapped_chunk, index_record);
//        for (int i = 0; i < index_record.limit(); i++) {
//            this.index_file_mapped_blocks[(int) which_mapped_chunk].put((int) (offset_in_mapped_chunk + i), index_record.array()[i]);
//        }
    }

    @Override
    public void run() {
        this.status = IO_thread_status.RUNNING;
        try {
            this.index_file_fd.truncate(AsyncIO.FILE_EXTEND_SIZE);
            this.index_file_size = AsyncIO.FILE_EXTEND_SIZE;

            remmap_index_block();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (; ; ) {
            AsyncIO_task task = null;
            try {
                task = (AsyncIO_task) this.blockingQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (this.status == IO_thread_status.CLOSING || task.write_counter == -1) {
                for (int i = 0; i < AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM; i++) {
                    unmap(index_file_mapped_blocks[i]);
                }
                AsyncIO.finished_thread.getAndIncrement();
                System.out.printf("io thread %d fallback %d times\n", thread_id, index_pwrite_times);
                break;
            }
            doIO(task);
        }
    }

    public IOThread(int thread_id, String file_prefix, int queue_num_per_file, int batch_size, int blocking_queue_size) {
        this.status = IO_thread_status.INITING;
        this.batch_size = batch_size;
        this.queue_num_per_file = queue_num_per_file;
        this.thread_id = thread_id;
        File data_file_name_f = new File(file_prefix + '_' + thread_id + ".data");
        File index_file_name_f = new File(file_prefix + '_' + thread_id + ".idx");
        try {
            data_file_fd = FileChannel.open(data_file_name_f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE, StandardOpenOption.SPARSE, StandardOpenOption.TRUNCATE_EXISTING);
            index_file_fd = FileChannel.open(index_file_name_f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE, StandardOpenOption.SPARSE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        blockingQueue = new ArrayBlockingQueue(AsyncIO.MAX_BLOCKING_IO_TASK);
        this.queue_counter = new int[queue_num_per_file];
        for (int i = 0; i < queue_num_per_file; i++) {
            this.queue_counter[i] = 0;
        }
    }

    int thread_id;
    public int queue_num_per_file;
    public int batch_size;
    long data_file_size = 0;
    long data_file_offset = 0;
    long index_file_size = 0;
    public int queue_counter[];

    public FileChannel data_file_fd;
    public FileChannel index_file_fd;
    BlockingQueue blockingQueue;
    IO_thread_status status;

    long current_index_mapped_start_offset = 0;
    long current_index_mapped_end_offset = 0;
    int index_mapped_block_write_counter[] = new int[AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM];
    long index_pwrite_times = 0;
    MappedByteBuffer index_file_mapped_blocks[] = new MappedByteBuffer[AsyncIO.MAX_INDEX_MAPPED_BLOCK_NUM];


    public AsyncIO_task task = new AsyncIO_task(DefaultQueueStoreImpl.SUBMIT_BATCH_SIZE);
    public ReentrantLock putTaskLock = new ReentrantLock();

}
