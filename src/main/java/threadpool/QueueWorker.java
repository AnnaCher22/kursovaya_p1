package threadpool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Рабочий поток, обрабатывающий задачи из своей очереди.
 * Завершается, если простаивает дольше заданного времени.
 */
public class QueueWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(QueueWorker.class);

    private final BlockingQueue<Runnable> taskQueue;
    private final int workerId;

    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private volatile boolean running = true;

    /**
     * Конструктор рабочего потока.
     *
     * @param taskQueue      очередь задач
     * @param workerId       идентификатор потока
     * @param keepAliveTime  время простоя перед завершением
     * @param timeUnit       единицы измерения времени
     */
    public QueueWorker(BlockingQueue<Runnable> taskQueue, int workerId, long keepAliveTime, TimeUnit timeUnit) {
        this.taskQueue = taskQueue;
        this.workerId = workerId;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
    }

    @Override
    public void run() {
        logger.info("Worker-{} запущен", workerId);
        try {
            while (running) {
                // Пытаемся взять задачу с тайм-аутом
                Runnable task = taskQueue.poll(keepAliveTime, timeUnit);
                if (task != null) {
                    logger.debug("Worker-{} получил задачу", workerId);
                    try {
                        task.run();
                    } catch (Exception e) {
                        logger.error("Ошибка при выполнении задачи Worker-{}: {}", workerId, e.getMessage());
                    }
                } else {
                    // Если задач не было в течение времени ожидания — завершаемся
                    logger.info("Worker-{} завершает работу после простоя", workerId);
                    running = false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Worker-{} был прерван", workerId);
        }
    }

    /**
     * Принудительное завершение потока (можно использовать позже при shutdown).
     */
    public void stop() {
        this.running = false;
    }
}