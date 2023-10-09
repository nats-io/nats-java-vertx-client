package io.nats.vertx;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ExecutorWrapperExecutorService extends AbstractExecutorService {
    private Executor executor;

    public ExecutorWrapperExecutorService(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }
}
