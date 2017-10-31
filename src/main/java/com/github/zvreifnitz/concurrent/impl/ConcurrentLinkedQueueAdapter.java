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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class ConcurrentLinkedQueueAdapter<T> implements RelaxedQueue<T> {

    private final ConcurrentLinkedQueue<T> underlyingQueue;
    private final int defaultEnqueueAllSize;

    public ConcurrentLinkedQueueAdapter(final int defaultEnqueueAllSize) {
        if (defaultEnqueueAllSize < 1) {
            throw new IllegalArgumentException("defaultEnqueueAllSize must be at least 1");
        }
        this.underlyingQueue = new ConcurrentLinkedQueue<>();
        this.defaultEnqueueAllSize = defaultEnqueueAllSize;
    }

    @Override
    public <I extends T> int enqueueAll(final I[] items) {
        if ((items == null) || (items.length == 0)) {
            return 0;
        }
        final List<T> collection = new ArrayList<>(items.length);
        for (int i = 0; i < items.length; i++) {
            collection.add(checkItem(items[i]));
        }
        this.underlyingQueue.addAll(collection);
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
        this.underlyingQueue.addAll(collection);
        return collection.size();
    }

    @Override
    public void enqueue(final T item) {
        this.underlyingQueue.add(checkItem(item));
    }

    @Override
    public T dequeue() {
        return this.underlyingQueue.poll();
    }

    @Override
    public Iterable<T> dequeueAll() {
        final List<T> result = new ArrayList<>(this.underlyingQueue.size() << 1);
        T item = null;
        while ((item = this.underlyingQueue.poll()) != null) {
            result.add(item);
        }
        return result;
    }

    private static <I> I checkItem(final I item) {
        if (item == null) {
            throw new NullPointerException("item");
        }
        return item;
    }
}
