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

package com.github.zvreifnitz.concurrent.comparison;

import com.github.zvreifnitz.concurrent.RelaxedQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public final class RingBufferRelaxedQueue<T> implements RelaxedQueue<T> {

    private static final VarHandle WriteReservationHandle;
    private static final VarHandle WriteCommitHandle;

    private static final VarHandle ReadReservationHandle;
    private static final VarHandle ReadCommitHandle;
    private static final VarHandle ReadCacheHandle;

    static {
        try {
            final MethodHandles.Lookup l = MethodHandles.lookup();
            WriteReservationHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "writeReservation", long.class);
            WriteCommitHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "writeCommit", long.class);
            ReadReservationHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "readReservation", long.class);
            ReadCommitHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "readCommit", long.class);
            ReadCacheHandle = l.findVarHandle(RingBufferRelaxedQueue.class, "readCache", long.class);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    private final T[] storage;
    private final int storageIndexMask;
    private final int size;
    private final Iterable<T> emptyIterable = new EmptyIterable<>();

    private volatile long readCache = 0L;

    @jdk.internal.vm.annotation.Contended
    private volatile long writeReservation = 0L;
    @jdk.internal.vm.annotation.Contended
    private volatile long writeCommit = 0L;

    @jdk.internal.vm.annotation.Contended
    private volatile long readReservation = 0L;
    @jdk.internal.vm.annotation.Contended
    private volatile long readCommit = 0L;


    public RingBufferRelaxedQueue(final Class<T> clazz, final int size) {
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
        @SuppressWarnings("unchecked") final T[] storageTmp = (T[])Array.newInstance(clazz, sizePow2);
        this.storage = storageTmp;
        this.size = size;
        this.storageIndexMask = (sizePow2 - 1);
    }

    @Override
    public <I extends T> int enqueueAll(final I[] items) {
        if ((items == null) || (items.length == 0) || (items.length > this.size)) {
            return 0;
        }
        for (int i = 0; i < items.length; i++) {
            checkItem(items[i]);
        }

        final Reservation reservation = this.reserveForWrite(items.length);
        int index = (((int)reservation.start) & this.storageIndexMask);
        for (int i = 0; i < items.length; i++) {
            this.storage[index] = items[i];
            index = ((index + 1) & this.storageIndexMask);
        }
        this.commitWrite(reservation);

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
        int count = 0;
        final List<T> collection = new LinkedList<>();
        while (iterator.hasNext()) {
            collection.add(checkItem(iterator.next()));
            count++;
        }
        if ((count == 0) || (count > this.size)) {
            return 0;
        }

        final Reservation reservation = this.reserveForWrite(count);
        int index = (((int)reservation.start) & this.storageIndexMask);
        for (final T item : collection) {
            this.storage[index] = item;
            index = ((index + 1) & this.storageIndexMask);
        }
        this.commitWrite(reservation);
        return count;
    }

    @Override
    public void enqueue(final T item) {
        checkItem(item);
        final Reservation reservation = this.reserveForWrite(1);
        this.storage[((int)reservation.start) & this.storageIndexMask] = item;
        this.commitWrite(reservation);
    }

    @Override
    public T dequeue() {
        final Reservation reservation = this.reserveForRead(1);
        if (reservation.size == 0) {
            return null;
        }
        final T result = this.readAndNullify(((int)reservation.start) & this.storageIndexMask);
        this.commitRead(reservation);
        return result;
    }

    @Override
    public Iterable<T> dequeueAll() {
        final Reservation reservation = this.reserveForRead(this.size);
        if (reservation.size == 0) {
            return this.emptyIterable;
        }
        final List<T> result = new LinkedList<>();
        int index = (((int)reservation.start) & this.storageIndexMask);
        for (int i = 0; i < reservation.size; i++) {
            result.add(this.readAndNullify(index));
            index = ((index + 1) & this.storageIndexMask);
        }
        this.commitRead(reservation);
        return result;
    }

    private T readAndNullify(final int index) {
        try {
            return this.storage[index];
        } finally {
            this.storage[index] = null;
        }
    }

    private Reservation reserveForWrite(final int desiredSize) {
        while (true) {
            final long cachedRead = (long)ReadCacheHandle.getOpaque(this);
            final long currentWrite = (long)WriteReservationHandle.getAcquire(this);
            final long desiredWrite = (currentWrite + desiredSize);
            if ((desiredWrite - cachedRead) <= this.size) {
                if (WriteReservationHandle.weakCompareAndSetRelease(this, currentWrite, desiredWrite)) {
                    return new Reservation(currentWrite, desiredWrite, desiredSize);
                }
            } else {
                ReadCacheHandle.setOpaque(this, ReadCommitHandle.getAcquire(this));
            }
        }
    }

    private void commitWrite(final Reservation reservation) {
        final long start = reservation.start;
        while ((long)WriteCommitHandle.getAcquire(this) != start) {
            Thread.onSpinWait();
        }
        WriteCommitHandle.setRelease(this, reservation.end);
    }

    private Reservation reserveForRead(final int maxSize) {
        final long commitWrite = (long)WriteCommitHandle.getAcquire(this);
        final long currentRead = (long)ReadReservationHandle.getAcquire(this);
        final long desiredRead = Math.min(currentRead + maxSize, commitWrite);
        if ((desiredRead > currentRead)
                && ReadReservationHandle.weakCompareAndSetRelease(this, currentRead, desiredRead)) {
            return new Reservation(currentRead, desiredRead, (int)(desiredRead - currentRead));
        } else {
            return new Reservation(currentRead, currentRead, 0);
        }
    }

    private void commitRead(final Reservation reservation) {
        final long start = reservation.start;
        while ((long)ReadCommitHandle.getAcquire(this) != start) {
            Thread.onSpinWait();
        }
        ReadCommitHandle.setRelease(this, reservation.end);
    }

    private static <I> I checkItem(final I item) {
        if (item == null) {
            throw new NullPointerException("item");
        }
        return item;
    }

    private final static class Reservation {
        private long start;
        private long end;
        private int size;

        private Reservation(final long start, final long end, final int size) {
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
