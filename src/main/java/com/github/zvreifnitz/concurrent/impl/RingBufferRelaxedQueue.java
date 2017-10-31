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

package com.github.zvreifnitz.concurrent.impl;

import com.github.zvreifnitz.concurrent.RelaxedQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class RingBufferRelaxedQueue<T> implements RelaxedQueue<T> {

    private static final VarHandle WriteReservationHandle;
    private static final VarHandle WriteCommitHandle;

    private static final VarHandle ReadReservationHandle;
    private static final VarHandle ReadCommitHandle;

    static {
        try {
            final MethodHandles.Lookup l = MethodHandles.lookup();
            WriteReservationHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "writeReservation", long.class);
            WriteCommitHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "writeCommit", long.class);
            ReadReservationHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "readReservation", long.class);
            ReadCommitHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "readCommit", long.class);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    private final T[] storage;
    private final int storageIndexMask;
    private final int size;
    private final int defaultEnqueueAllSize;
    private final Iterable<T> emptyIterable = new EmptyIterable<>();
    private final ThreadLocal<LongWrapper> writeCommitLocal = ThreadLocal.withInitial(this::createWriteCommitLocal);
    private final ThreadLocal<LongWrapper> readCommitLocal = ThreadLocal.withInitial(this::createReadCommitLocal);

    @jdk.internal.vm.annotation.Contended
    private volatile long writeReservation = 0L;
    @jdk.internal.vm.annotation.Contended
    private volatile long writeCommit = 0L;

    @jdk.internal.vm.annotation.Contended
    private volatile long readReservation = 0L;
    @jdk.internal.vm.annotation.Contended
    private volatile long readCommit = 0L;


    public RingBufferRelaxedQueue(final Class<T> clazz, final int size, final int defaultEnqueueAllSize) {
        if (clazz == null) {
            throw new NullPointerException("Parameter clazz must not be null");
        }
        if (size < 1) {
            throw new IllegalArgumentException("Parameter size must be grater than 0");
        }
        int sizePow2 = 0;
        for (int i = 0; i < 31; i++) {
            final int sizePow2Tmp = (1 << i);
            if (sizePow2Tmp >= size) {
                sizePow2 = sizePow2Tmp;
                break;
            }
        }
        if (sizePow2 < 1) {
            throw new IllegalArgumentException("Parameter size is too big");
        }
        @SuppressWarnings("unchecked") final T[] storageTmp = (T[]) Array.newInstance(clazz, sizePow2);
        this.storage = storageTmp;
        this.size = size;
        this.storageIndexMask = (sizePow2 - 1);
        this.defaultEnqueueAllSize = defaultEnqueueAllSize;
    }

    @Override
    public <I extends T> int enqueueAll(final I[] items) {
        if ((items == null) || (items.length == 0) || (items.length > this.size)) {
            return 0;
        }
        for (int i = 0; i < items.length; i++) {
            checkItem(items[i]);
        }

        final ReservationInterval interval = this.reserveForWrite(items.length);
        int index = (((int) interval.start) & this.storageIndexMask);
        for (int i = 0; i < items.length; i++) {
            this.storage[index] = items[i];
            index = ((index + 1) & this.storageIndexMask);
        }
        this.commitWrite(interval);

        return items.length;
    }

    @Override
    public int enqueueAll(final Iterable<? extends T> items) {
        if (items == null) {
            return 0;
        }
        return this.enqueueRemaining(items.iterator());
    }

    @Override
    public int enqueueRemaining(final Iterator<? extends T> iterator) {
        if (iterator == null) {
            return 0;
        }
        final List<T> collection = new ArrayList<>(this.defaultEnqueueAllSize);
        while (iterator.hasNext()) {
            collection.add(checkItem(iterator.next()));
        }
        if ((collection.size() == 0) || (collection.size() > this.size)) {
            return 0;
        }

        final ReservationInterval interval = this.reserveForWrite(collection.size());
        int index = (((int) interval.start) & this.storageIndexMask);
        for (int i = 0; i < collection.size(); i++) {
            this.storage[index] = collection.get(i);
            index = ((index + 1) & this.storageIndexMask);
        }
        this.commitWrite(interval);

        return collection.size();
    }

    @Override
    public void enqueue(final T item) {
        checkItem(item);
        final ReservationInterval interval = this.reserveForWrite(1);
        this.storage[((int) interval.start) & this.storageIndexMask] = item;
        this.commitWrite(interval);
    }

    @Override
    public T dequeue() {
        final ReservationInterval interval = this.reserveForRead(1);
        if (interval.size == 0) {
            return null;
        }
        final T result = this.readAndNullify(((int) interval.start) & this.storageIndexMask);
        this.commitRead(interval);
        return result;
    }

    @Override
    public Iterable<T> dequeueAll() {
        final ReservationInterval interval = this.reserveForRead(this.size);
        if (interval.size == 0) {
            return this.emptyIterable;
        }
        final List<T> result = new ArrayList<>(interval.size);
        int index = (((int) interval.start) & this.storageIndexMask);
        for (int i = 0; i < interval.size; i++) {
            result.add(this.readAndNullify(index));
            index = ((index + 1) & this.storageIndexMask);
        }
        this.commitRead(interval);
        return result;
    }

    private LongWrapper createWriteCommitLocal() {
        final LongWrapper result = new LongWrapper();
        result.value = (long) WriteCommitHandle.getAcquire(this);
        return result;
    }

    private LongWrapper createReadCommitLocal() {
        final LongWrapper result = new LongWrapper();
        result.value = (long) ReadCommitHandle.getAcquire(this);
        return result;
    }

    private T readAndNullify(final int index) {
        try {
            return this.storage[index];
        } finally {
            this.storage[index] = null;
        }
    }

    private ReservationInterval reserveForWrite(final int desiredSize) {
        final LongWrapper readCommitLocal = this.readCommitLocal.get();
        while (true) {
            final long currentWrite = (long) WriteReservationHandle.getAcquire(this);
            final long desiredWrite = (currentWrite + desiredSize);
            if ((desiredWrite - readCommitLocal.value) <= this.size) {
                if (WriteReservationHandle.weakCompareAndSetRelease(this, currentWrite, desiredWrite)) {
                    return new ReservationInterval(currentWrite, desiredWrite, desiredSize);
                }
            } else {
                readCommitLocal.value = (long) ReadCommitHandle.getAcquire(this);
            }
        }
    }

    private void commitWrite(final ReservationInterval interval) {
        final long start = interval.start;
        while ((long) WriteCommitHandle.getAcquire(this) != start) {
            Thread.onSpinWait();
        }
        WriteCommitHandle.setRelease(this, interval.end);
    }

    private ReservationInterval reserveForRead(final int maxSize) {
        final LongWrapper writeCommitLocal = this.writeCommitLocal.get();
        while (true) {
            final long currentRead = (long) ReadReservationHandle.getAcquire(this);
            final long desiredRead = Math.min(currentRead + maxSize, writeCommitLocal.value);
            if (desiredRead > currentRead) {
                if (ReadReservationHandle.weakCompareAndSetRelease(this, currentRead, desiredRead)) {
                    return new ReservationInterval(currentRead, desiredRead, (int) (desiredRead - currentRead));
                }
            } else {
                final long commitWrite = (long) WriteCommitHandle.getAcquire(this);
                if (commitWrite > writeCommitLocal.value) {
                    writeCommitLocal.value = commitWrite;
                } else {
                    return new ReservationInterval(currentRead, currentRead, 0);
                }
            }
        }
    }

    private void commitRead(final ReservationInterval interval) {
        final long start = interval.start;
        while ((long) ReadCommitHandle.getAcquire(this) != start) {
            Thread.onSpinWait();
        }
        ReadCommitHandle.setRelease(this, interval.end);
    }

    private static <I> I checkItem(final I item) {
        if (item == null) {
            throw new NullPointerException("item");
        }
        return item;
    }

    private final static class LongWrapper {
        private long value;
    }

    private final static class ReservationInterval {
        private long start;
        private long end;
        private int size;

        private ReservationInterval(final long start, final long end, final int size) {
            this.start = start;
            this.end = end;
            this.size = size;
        }
    }

    private final static class EmptyIterable<T> implements Iterable<T>, Iterator<T> {

        @Override
        public Iterator<T> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    }
}
