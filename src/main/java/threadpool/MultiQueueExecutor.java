package threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Кастомный пул потоков с несколькими очередями задач.
 * Реализует интерфейс CustomExecutor.
 */
public class MultiQueueExecutor implements CustomExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MultiQueueExecutor.class);

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int minSpareThreads;

    private final List<QueueWorker> workers;
    private final List<BlockingQueue<Runnable>> taskQueues;

    private final AtomicInteger queueIndex = new AtomicInteger(0);
    private volatile boolean isShutdown = false;

    public MultiQueueExecutor(int corePoolSize, int maxPoolSize, int queueSize,
                              long keepAliveTime, TimeUnit timeUnit, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueSize = queueSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.minSpareThreads = minSpareThreads;

        this.taskQueues = new ArrayList<>();
        this.workers = new ArrayList<>();

        logger.info("Инициализация пула потоков: core={}, max={}, queueSize={}, keepAlive={} {}",
                corePoolSize, maxPoolSize, queueSize, keepAliveTime, timeUnit.name());

        for (int i = 0; i < corePoolSize; i++) {
            createWorker(i);
        }
    }

    private void createWorker(int index) {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        QueueWorker worker = new QueueWorker(queue, index, keepAliveTime, timeUnit);
        taskQueues.add(queue);
        workers.add(worker);
        new Thread(worker, "Worker-" + index).start();
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            throw new RejectedExecutionException("Пул потоков завершён, задача отклонена.");
        }

        int index = queueIndex.getAndIncrement() % taskQueues.size();
        BlockingQueue<Runnable> queue = taskQueues.get(index);
        if (!queue.offer(command)) {
            throw new RejectedExecutionException("Очередь переполнена, задача отклонена.");
        }

        logger.debug("Задача отправлена в очередь {}", index);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (isShutdown) {
            throw new RejectedExecutionException("Пул потоков завершён, задача отклонена.");
        }

        FutureTask<T> futureTask = new FutureTask<>(callable);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        logger.info("Пул переводится в режим завершения (shutdown)");
    }

    @Override
    public void shutdownNow() {
        isShutdown = true;
        logger.warn("Принудительное завершение всех потоков (shutdownNow)");
        for (QueueWorker worker : workers) {
            worker.stop(); // метод уже есть
        }
    }

    public int getWorkerCount() {
        return workers.size();
    }
}