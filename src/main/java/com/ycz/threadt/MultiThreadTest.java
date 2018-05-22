package com.ycz.threadt;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadTest {

    private static final Logger logger = LoggerFactory.getLogger(MultiThreadTest.class);

    //静态变量模拟公共资源
    private static int counter = 0;

    //操作计数
    public static void plus() {
        //计数器+1 为啥当有处理时长的时候才会出现并发问题，等待时长模拟了大量线程同时处理某一项共享数据的操作，出现了对资源的竞争
        //导致同一时间内，多个线程拿到了相同的数据做处理，所以最后的结果出现问题
        counter = counter + 1;
        logger.info("counter {}", counter);
        try {
            Thread.sleep(2000);//当有处理时长的时候 并发问题出现
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class CountPlus extends Thread {

        private CountDownLatch countDownLatch;

        @Override
        public void run() {
            for (int i = 0; i < 20; i++) {
                plus();
            }
            logger.info("{} 线程执行完毕 {}", Thread.currentThread().getName(), counter);
            countDownLatch.countDown();
        }

        public CountPlus(String name, CountDownLatch countDownLatch) {
            super(name);
            this.countDownLatch = countDownLatch;
        }
    }

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(6);
        CountPlus t1 = new CountPlus("线程1", countDownLatch);
        t1.start();
        CountPlus t2 = new CountPlus("线程2", countDownLatch);
        t2.start();
        CountPlus t3 = new CountPlus("线程3", countDownLatch);
        t3.start();
        CountPlus t4 = new CountPlus("线程4", countDownLatch);
        t4.start();
        CountPlus t5 = new CountPlus("线程5", countDownLatch);
        t5.start();
        CountPlus t6 = new CountPlus("线程6", countDownLatch);
        t6.start();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("最终计数值为 {}", counter);
    }

}
