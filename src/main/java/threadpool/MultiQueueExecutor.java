package threadpool;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class CustomThreadPoolExecutor implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    private final ThreadFactory threadFactory;
    private final List<Worker> workers = new CopyOnWriteArrayList<>();
    private final AtomicInteger totalWorkers = new AtomicInteger(0);
    private final AtomicInteger busyWorkers = new AtomicInteger(0);
    private volatile boolean isShutdown = false;
    private final AtomicInteger nextQueueIndex = new AtomicInteger(0);
    private final ReentrantLock mainLock = new ReentrantLock();
    private final CustomRejectedExecutionHandler rejectedExecutionHandler;

    public CustomThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit timeUnit,
                                    int queueSize, int minSpareThreads, CustomThreadFactory threadFactory,
                                    CustomRejectedExecutionHandler rejectedExecutionHandler) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = threadFactory;
        this.rejectedExecutionHandler = rejectedExecutionHandler;

        for (int i = 0; i < corePoolSize; i++) {
            addWorker(true); // true = core-поток
        }
    }

    private void addWorker(boolean isCore) {
        mainLock.lock();
        try {
            if (isShutdown || totalWorkers.get() >= maxPoolSize) return;
            Worker worker = new Worker(queueSize, isCore);
            workers.add(worker);
            totalWorkers.incrementAndGet();
            threadFactory.newThread(worker).start();
        } finally {
            mainLock.unlock();
        }
    }

    void onTaskStart() {
        busyWorkers.incrementAndGet();
    }

    void onTaskComplete() {
        busyWorkers.decrementAndGet();
        checkMinSpareThreads();
    }

    void onWorkerTerminated(Worker worker) {
        mainLock.lock();
        try {
            workers.remove(worker);
            totalWorkers.decrementAndGet();
            checkMinSpareThreads();
        } finally {
            mainLock.unlock();
        }
    }

    private void checkMinSpareThreads() {
        mainLock.lock();
        try {
            if (isShutdown) return;
            int idle = totalWorkers.get() - busyWorkers.get();
            // Добавляем потоки, если свободных меньше minSpareThreads
            if (idle < minSpareThreads && totalWorkers.get() < maxPoolSize) {
                addWorker();
            }
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            rejectedExecutionHandler.rejectedExecution(command, this);
            return;
        }

        int currentSize = workers.size();
        if (currentSize == 0) {
            rejectedExecutionHandler.rejectedExecution(command, this);
            return;
        }

        int startIdx = nextQueueIndex.getAndUpdate(i -> (i + 1) % currentSize);
        for (int i = 0; i < currentSize; i++) {
            int idx = (startIdx + i) % currentSize;
            Worker worker = workers.get(idx);
            if (worker.offerTask(command)) {
                logTaskAccepted(idx, command);
                return;
            }
        }

        if (totalWorkers.get() < maxPoolSize) {
            addWorker();
            execute(command);
            return;
        }

        rejectedExecutionHandler.rejectedExecution(command, this);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        // Не прерываем потоки явно, позволяя им завершить задачи из очереди
    }

    @Override
    public void shutdownNow() {
        isShutdown = true;
        workers.forEach(worker -> {
            worker.stop(); // Прерываем текущие задачи
            worker.queue.clear(); // Очищаем очередь
        });
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.nanoTime() + unit.toNanos(timeout);
        for (Worker worker : workers) {
            while (!worker.queue.isEmpty()) {
                if (System.nanoTime() > endTime) {
                    return false;
                }
                Thread.sleep(100);
            }
        }
        return true;
    }

    private void logTaskAccepted(int queueId, Runnable task) {
        System.out.printf("[Pool] Task accepted into queue #%d: %s%n", queueId, task);
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private volatile boolean isRunning = true;
        private volatile Thread currentThread;
        private final boolean isCoreThread; // Флаг для определения типа потока

        Worker(int queueSize, boolean isCoreThread) {
            this.queue = new LinkedBlockingQueue<>(queueSize);
            this.isCoreThread = isCoreThread;
        }

        public boolean offerTask(Runnable task) {
            return queue.offer(task);
        }

        void stop() {
            isRunning = false;
            if (currentThread != null) {
                currentThread.interrupt();
            }
        }

        @Override
        public void run() {
            currentThread = Thread.currentThread();
            try {
                while (isRunning && !currentThread.isInterrupted()) {
                    Runnable task = queue.poll(
                            isCoreThread ? Long.MAX_VALUE : keepAliveTime, // Для core-потоков нет таймаута
                            TimeUnit.NANOSECONDS
                    );

                    if (task != null) {
                        onTaskStart();
                        try {
                            System.out.printf("[Worker] %s executes %s%n", currentThread.getName(), task);
                            task.run();
                        } finally {
                            onTaskComplete();
                        }
                    } else {
                        // Для дополнительных потоков: завершаем если пул остановлен или превышен corePoolSize
                        if (!isCoreThread && (isShutdown || totalWorkers.get() > corePoolSize)) {
                            System.out.printf("[Worker] %s idle timeout, stopping.%n", currentThread.getName());
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                onWorkerTerminated(this);
                System.out.printf("[Worker] %s terminated.%n", currentThread.getName());
            }
        }


    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long endTime = System.nanoTime() + unit.toNanos(timeout);
            while (true) {
                boolean allQueuesEmpty = true;
                for (Worker worker : workers) {
                    if (!worker.queue.isEmpty()) {
                        allQueuesEmpty = false;
                        break;
                    }
                }
                if (allQueuesEmpty || System.nanoTime() > endTime) {
                    break;
                }
                Thread.sleep(100);
            }
            return workers.isEmpty();
        }
    }
}