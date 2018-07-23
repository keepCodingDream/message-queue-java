package io.openmessaging.io;

public class AsyncIO_task {
    byte data[][];
    long which_queues[];
    public int write_counter;
    public AsyncIO_task(){}
    public AsyncIO_task(int buffer_size){
        this.data = new byte[buffer_size][];
        this.which_queues = new long[buffer_size];
        write_counter = 0;
    }
    public void put(long which_queue, byte[] message){
        data[write_counter] = message;
        which_queues[write_counter] = which_queue;
        write_counter++;
    }
}
