package com.github.zvreifnitz.concurrent;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

final class TestScenario {

    private final static Object Msg = new Object();

    static long exec(final RelaxedQueue<Object> requestQueue, final RelaxedQueue<Object> responseQueue,
                     final int numOfThreads, final int numOfBatches, final int batchSize) {

        final List<Pinger> pingers = new ArrayList<>(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            pingers.add(new Pinger(requestQueue, responseQueue, numOfBatches, batchSize));
        }
        final List<Ponger> pongers = new ArrayList<>(numOfThreads);
        for (int i = 0; i < numOfThreads; i++) {
            pongers.add(new Ponger(requestQueue, responseQueue, numOfBatches, batchSize));
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

            if (this.batchSize > 0) {
                this.runInBatchMode();
            } else {
                this.runInNormalMode();
            }

            final long endNanos = currentNanos();
            this.result = (endNanos - startNanos);
        }

        private void runInNormalMode() {
            for (int batchIndex = 0; batchIndex < this.numOfBatches; batchIndex++) {
                this.requestQueue.enqueue(Msg);
                while (this.responseQueue.dequeue() != Msg) {
                    Thread.onSpinWait();
                }
            }
        }

        private void runInBatchMode() {
            for (int batchIndex = 0; batchIndex < this.numOfBatches; batchIndex++) {
                this.requestQueue.enqueueAll(this.batch);
                int remainingBatchSize = this.batchSize;
                while (remainingBatchSize > 0) {
                    remainingBatchSize -= this.processBatch(remainingBatchSize);
                }
            }
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

            if (this.batchSize > 0) {
                this.runInBatchMode();
            } else {
                this.runInNormalMode();
            }

            final long endNanos = currentNanos();
            this.result = (endNanos - startNanos);
        }

        private void runInNormalMode() {
            for (int batchIndex = 0; batchIndex < this.numOfBatches; batchIndex++) {
                while (this.requestQueue.dequeue() != Msg) {
                    Thread.onSpinWait();
                }
                this.responseQueue.enqueue(Msg);
            }
        }

        private void runInBatchMode() {
            for (int batchIndex = 0; batchIndex < this.numOfBatches; batchIndex++) {
                int remainingBatchSize = this.batchSize;
                while (remainingBatchSize > 0) {
                    remainingBatchSize -= this.processBatch(remainingBatchSize);
                }
            }
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
