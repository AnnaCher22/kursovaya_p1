package threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final String namePrefix;

    public CustomThreadFactory(String poolName) {
        this.namePrefix = poolName + "-worker-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + counter.getAndIncrement());
        System.out.printf("[ThreadFactory] Creating new thread: %s%n", thread.getName());
        return thread;
    }
}