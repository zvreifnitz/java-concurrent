/*
 * (C) Copyright 2017 zvreifnitz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.github.zvreifnitz.concurrent;

import com.github.zvreifnitz.concurrent.comparison.JCToolsMpmcArrayQueue;
import com.github.zvreifnitz.concurrent.comparison.JavaConcurrentLinkedQueue;
import com.github.zvreifnitz.concurrent.comparison.RingBufferRelaxedQueue;
import com.github.zvreifnitz.concurrent.impl.LinkedRelaxedQueue;
import com.github.zvreifnitz.concurrent.impl.LinkedRelaxedQueueWeak1;
import com.github.zvreifnitz.concurrent.impl.LinkedRelaxedQueueWeak2;
import org.openjdk.jmh.annotations.*;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-XX:-RestrictContended"})
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class RelaxedQueuePerfTest {

    private final static Object Msg = new Object();
    private final static int NumOfPingPongs = 10_000_000;

    @Param({"1", "2", "4", "8"})
    private int numOfThreads;
    @Param({"1", "10", "100", "1000"})
    private int batchSize;
    private int numOfBatches;

    private JavaConcurrentLinkedQueue<Object> javaConcurrentLinkedQueue_Req;
    private JavaConcurrentLinkedQueue<Object> javaConcurrentLinkedQueue_Resp;
    private RingBufferRelaxedQueue<Object> ringBufferRelaxedQueue_Req;
    private RingBufferRelaxedQueue<Object> ringBufferRelaxedQueue_Resp;
    private LinkedRelaxedQueue<Object> linkedRelaxedQueue_Req;
    private LinkedRelaxedQueue<Object> linkedRelaxedQueue_Resp;
    private LinkedRelaxedQueueWeak2<Object> linkedRelaxedQueueWeak2_Req;
    private LinkedRelaxedQueueWeak2<Object> linkedRelaxedQueueWeak2_Resp;
    private LinkedRelaxedQueueWeak1<Object> linkedRelaxedQueueWeak1_Req;
    private LinkedRelaxedQueueWeak1<Object> linkedRelaxedQueueWeak1_Resp;
    private JCToolsMpmcArrayQueue<Object> jcToolsMpmcArrayQueue_Req;
    private JCToolsMpmcArrayQueue<Object> jcToolsMpmcArrayQueue_Resp;

    public static void main(final String[] args) {
        final RelaxedQueuePerfTest test = new RelaxedQueuePerfTest();
        test.numOfThreads = 8;
        test.batchSize = 1;
        test.init();
        final long r1 = test.java_ConcurrentLinkedQueue();
        System.out.println("   concurrentLinkedQueue: " + (r1 / 1_000_000.0));
        final long r2 = test.jcTools_MpmcArrayQueue();
        System.out.println("   jcToolsMpmcArrayQueue: " + (r2 / 1_000_000.0));
        final long r3 = test.custom_RingBufferRelaxedQueue();
        System.out.println("  ringBufferRelaxedQueue: " + (r3 / 1_000_000.0));
        final long r4 = test.custom_LinkedRelaxedQueue();
        System.out.println("      linkedRelaxedQueue: " + (r4 / 1_000_000.0));
        final long r5 = test.custom_LinkedRelaxedQueueWeak1();
        System.out.println(" linkedRelaxedQueueWeak1: " + (r5 / 1_000_000.0));
        final long r6 = test.custom_LinkedRelaxedQueueWeak2();
        System.out.println(" linkedRelaxedQueueWeak2: " + (r6 / 1_000_000.0));
    }

    @Setup
    public void init() {
        this.numOfBatches = (NumOfPingPongs / (this.batchSize * this.numOfBatches));
        this.javaConcurrentLinkedQueue_Req = new JavaConcurrentLinkedQueue<>();
        this.javaConcurrentLinkedQueue_Resp = new JavaConcurrentLinkedQueue<>();
        this.ringBufferRelaxedQueue_Req = new RingBufferRelaxedQueue<>(Object.class, NumOfPingPongs);
        this.ringBufferRelaxedQueue_Resp = new RingBufferRelaxedQueue<>(Object.class, NumOfPingPongs);
        this.linkedRelaxedQueue_Req = new LinkedRelaxedQueue<>();
        this.linkedRelaxedQueue_Resp = new LinkedRelaxedQueue<>();
        this.linkedRelaxedQueueWeak1_Req = new LinkedRelaxedQueueWeak1<>();
        this.linkedRelaxedQueueWeak1_Resp = new LinkedRelaxedQueueWeak1<>();
        this.linkedRelaxedQueueWeak2_Req = new LinkedRelaxedQueueWeak2<>();
        this.linkedRelaxedQueueWeak2_Resp = new LinkedRelaxedQueueWeak2<>();
        this.jcToolsMpmcArrayQueue_Req = new JCToolsMpmcArrayQueue<>(NumOfPingPongs);
        this.jcToolsMpmcArrayQueue_Resp = new JCToolsMpmcArrayQueue<>(NumOfPingPongs);
    }

    //@Benchmark
    public long java_ConcurrentLinkedQueue() {
        return testImpl(this.javaConcurrentLinkedQueue_Req, this.javaConcurrentLinkedQueue_Resp);
    }

    //@Benchmark
    public long custom_RingBufferRelaxedQueue() {
        return testImpl(this.ringBufferRelaxedQueue_Req, this.ringBufferRelaxedQueue_Resp);
    }

    @Benchmark
    public long custom_LinkedRelaxedQueue() {
        return testImpl(this.linkedRelaxedQueue_Req, this.linkedRelaxedQueue_Resp);
    }

    @Benchmark
    public long custom_LinkedRelaxedQueueWeak1() {
        return testImpl(this.linkedRelaxedQueueWeak1_Req, this.linkedRelaxedQueueWeak1_Resp);
    }

    @Benchmark
    public long custom_LinkedRelaxedQueueWeak2() {
        return testImpl(this.linkedRelaxedQueueWeak2_Req, this.linkedRelaxedQueueWeak2_Resp);
    }

    //@Benchmark
    public long jcTools_MpmcArrayQueue() {
        return testImpl(this.jcToolsMpmcArrayQueue_Req, this.jcToolsMpmcArrayQueue_Resp);
    }

    private long testImpl(final RelaxedQueue<Object> requestQueue, final RelaxedQueue<Object> responseQueue) {
        final List<Pinger> pingers = new ArrayList<>(this.numOfThreads);
        for (int i = 0; i < this.numOfThreads; i++) {
            pingers.add(new Pinger(requestQueue, responseQueue, this.numOfBatches, this.batchSize));
        }
        final List<Ponger> pongers = new ArrayList<>(this.numOfThreads);
        for (int i = 0; i < this.numOfThreads; i++) {
            pongers.add(new Ponger(requestQueue, responseQueue, this.numOfBatches, this.batchSize));
        }

        pongers.forEach(Ponger::startTest);
        pingers.forEach(Pinger::startTest);

        pongers.forEach(Ponger::awaitTestDone);
        pingers.forEach(Pinger::awaitTestDone);

        return pingers.stream().max(Comparator.comparingLong(Pinger::getResult)).get().getResult();
    }

    private final static class Pinger extends Thread {

        private final RelaxedQueue<Object> requestQueue;
        private final RelaxedQueue<Object> responseQueue;
        private final int numOfBatches;
        private final int batchSize;
        private final Object[] batch;

        private volatile long result;

        private Pinger(final RelaxedQueue<Object> requestQueue, final RelaxedQueue<Object> responseQueue,
                       final int numOfBatches, final int batchSize) {
            this.requestQueue = requestQueue;
            this.responseQueue = responseQueue;
            this.numOfBatches = numOfBatches;
            this.batchSize = batchSize;
            this.batch = new Object[batchSize];
            for (int i = 0; i < batchSize; i++) {
                this.batch[i] = Msg;
            }
        }

        @Override
        public void run() {
            final long startNanos = currentNanos();

            for (int batchIndex = 0; batchIndex < this.numOfBatches; batchIndex++) {
                this.requestQueue.enqueueAll(this.batch);
                int remainingBatchSize = this.batchSize;
                while (remainingBatchSize > 0) {
                    remainingBatchSize -= this.processBatch(remainingBatchSize);
                }
            }

            final long endNanos = currentNanos();
            this.result = (endNanos - startNanos);
        }

        void startTest() {
            this.start();
        }

        void awaitTestDone() {
            try {
                this.join();
            } catch (InterruptedException e) {
            }
        }

        long getResult() {
            return this.result;
        }

        private int processBatch(final int size) {
            int result = 0;
            final Iterator<Object> requests = this.responseQueue.dequeueAll().iterator();
            while (requests.hasNext() && (result < size)) {
                result += ((requests.next() == Msg) ? 1 : 0);
            }
            this.responseQueue.enqueueRemaining(requests);
            return result;
        }
    }

    private final static class Ponger extends Thread {

        private final RelaxedQueue<Object> requestQueue;
        private final RelaxedQueue<Object> responseQueue;
        private final int numOfBatches;
        private final int batchSize;

        private volatile long result;

        private Ponger(final RelaxedQueue<Object> requestQueue, final RelaxedQueue<Object> responseQueue,
                       final int numOfBatches, final int batchSize) {
            this.requestQueue = requestQueue;
            this.responseQueue = responseQueue;
            this.numOfBatches = numOfBatches;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            final long startNanos = currentNanos();

            for (int batchIndex = 0; batchIndex < this.numOfBatches; batchIndex++) {
                int remainingBatchSize = this.batchSize;
                while (remainingBatchSize > 0) {
                    remainingBatchSize -= this.processBatch(remainingBatchSize);
                }
            }

            final long endNanos = currentNanos();
            this.result = (endNanos - startNanos);
        }

        void startTest() {
            this.start();
        }

        void awaitTestDone() {
            try {
                this.join();
            } catch (InterruptedException e) {
            }
        }

        long getResult() {
            return this.result;
        }

        private int processBatch(final int size) {
            final List<Object> result = new ArrayList<>(size);
            final Iterator<Object> requests = this.requestQueue.dequeueAll().iterator();
            while (requests.hasNext() && (result.size() < size)) {
                result.add(requests.next());
            }
            this.requestQueue.enqueueRemaining(requests);
            return this.responseQueue.enqueueAll(result);
        }
    }

    private static long currentNanos() {
        try {
            VarHandle.fullFence();
            return System.nanoTime();
        } finally {
            VarHandle.fullFence();
        }
    }
}
