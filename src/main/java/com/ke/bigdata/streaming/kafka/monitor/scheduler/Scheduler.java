package com.ke.bigdata.streaming.kafka.monitor.scheduler;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author hy
 * @date 2019/8/1
 * @desc
 */
public class Scheduler {
    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private final long millisseconds;
    private volatile boolean stopped;
    private ExecutorService pool;
    private List<Runnable> runnables;

    public Scheduler(long period, TimeUnit timeUnit) {
        this.millisseconds = timeUnit.toMillis(period);
        this.runnables = Lists.newArrayList();
        this.stopped = false;
    }

    public void addTask(Runnable task) {
        runnables.add(task);
    }

    public void start() {

        this.pool = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("scheduler");
            return thread;
        });
        pool.execute(() -> {
            while(!stopped) {
                for (Runnable runnable : runnables) {
                    long t1 = System.currentTimeMillis();
                    runnable.run();
                    long t2 = System.currentTimeMillis();
                    long shouldSleep = millisseconds - (t2 - t1);
                    while (shouldSleep < 0) {
                        shouldSleep = millisseconds + shouldSleep;
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(shouldSleep);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
    }

    public void stop() throws InterruptedException {
        stopped = true;
        pool.awaitTermination(2, TimeUnit.SECONDS);
        pool.shutdownNow();
    }
}
