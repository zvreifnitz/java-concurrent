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
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class JCToolsMpmcArrayQueue<T> implements RelaxedQueue<T> {

    private final MpmcArrayQueue<T> underlyingQueue;

    public JCToolsMpmcArrayQueue(final int size) {
        this.underlyingQueue = new MpmcArrayQueue<>(size);
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
        int count = 0;
        final List<T> collection = new LinkedList<>();
        while (iterator.hasNext()) {
            collection.add(checkItem(iterator.next()));
            count++;
        }
        this.underlyingQueue.addAll(collection);
        return count;
    }

    @Override
    public void enqueue(final T item) {
        this.underlyingQueue.offer(checkItem(item));
    }

    @Override
    public T dequeue() {
        return this.underlyingQueue.relaxedPoll();
    }

    @Override
    public Iterable<T> dequeueAll() {
        final ConsumingLinkedList<T> result = new ConsumingLinkedList<>();
        this.underlyingQueue.drain(result);
        return result;
    }

    private static <I> I checkItem(final I item) {
        if (item == null) {
            throw new NullPointerException("item");
        }
        return item;
    }

    private final static class ConsumingLinkedList<I> extends LinkedList<I> implements MessagePassingQueue.Consumer<I> {

        @Override
        public void accept(final I item) {
            this.add(item);
        }
    }
}
