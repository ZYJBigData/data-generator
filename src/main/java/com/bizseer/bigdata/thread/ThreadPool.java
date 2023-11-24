package com.bizseer.bigdata.thread;

import java.util.concurrent.*;

public class ThreadPool {
    public static void main(String[] args) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(3, 3,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(10), r -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    return t;
                });
        //获取阻塞队列中的值
        int size = threadPoolExecutor.getQueue().size();
        System.out.println("没提交任务时，队列中的值：" + size);
        //实现异步操作
        for (int i = 0; i < 13; i++) {
            threadPoolExecutor.submit(() -> {
                System.out.println("我在执行......");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            int size1 = threadPoolExecutor.getQueue().size();
            System.out.println("提交后队列的数===" + size1);
        }
    }
}
