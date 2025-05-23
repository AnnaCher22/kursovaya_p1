package threadpool;


import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Интерфейс пользовательского пула потоков.
 * Требуется реализовать для совместимости с Executor и поддержки submit().
 */
public interface CustomExecutor extends Executor {

    /**
     * Отправка задачи на выполнение (часть интерфейса Executor).
     *
     * @param command задача Runnable
     */
    @Override
    void execute(Runnable command);

    /**
     * Отправка задачи Callable на выполнение, возвращает Future.
     *
     * @param callable задача с результатом
     * @param <T> тип результата
     * @return Future, позволяющий получить результат позже
     */
    <T> Future<T> submit(Callable<T> callable);

    /**
     * Мягкое завершение работы пула — больше не принимает задачи.
     * Воркеры завершаются после выполнения текущих задач.
     */
    void shutdown();

    /**
     * Принудительное завершение работы пула — отменяет все потоки.
     * Текущие задачи прерываются, если это возможно.
     */
    void shutdownNow();
}