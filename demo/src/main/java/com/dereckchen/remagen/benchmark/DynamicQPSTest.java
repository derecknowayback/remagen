package com.dereckchen.remagen.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class DynamicQPSTest {


    public static void testQPS(Runnable runnable, int qps) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(12);
        startRequests(executorService, qps, runnable);
        executorService.shutdownNow();
    }

    private static void startRequests(ScheduledExecutorService executorService, int qps, Runnable runnable) {
        long intervalNanoseconds = 1_000_000_000L / qps;
        List<ScheduledFuture<?>> futures = new ArrayList<>();
        long currentTime = 0;
        int cnt = 0;

        // 以纳秒为单位进行任务调度
        while (currentTime < 1_000_000_000) {
            ScheduledFuture<?> schedule = executorService.schedule(runnable, currentTime, TimeUnit.NANOSECONDS);
            futures.add(schedule);
            currentTime += intervalNanoseconds;
            cnt++;
        }
        futures.forEach(scheduledFuture -> {
            try {
                scheduledFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("执行失败", e);
            }
        });
        log.warn("QPS: {} 执行完毕, 共发送{}次请求", qps, cnt);
    }
}