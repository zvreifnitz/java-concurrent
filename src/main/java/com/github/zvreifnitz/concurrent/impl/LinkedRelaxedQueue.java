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
import java.util.Iterator;
import java.util.NoSuchElementException;

/*
 * Variation of "Intrusive MPSC node-based queue"
 * (author: D. Vyukov, link: http://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue)
 */
public final class LinkedRelaxedQueue<T> implements RelaxedQueue<T> {

    private static final VarHandle LastHandle;
    private static final VarHandle FirstHandle;

    static {
        try {
            final MethodHandles.Lookup l = MethodHandles.lookup();
            LastHandle = l.findVarHandle(LinkedRelaxedQueue.class, "last", Node.class);
            FirstHandle = l.findVarHandle(LinkedRelaxedQueue.class, "first", Node.class);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    private final Iterable<T> emptyIterable = new EmptyIterable<>();

    @jdk.internal.vm.annotation.Contended
    private volatile Node<T> first = new Node<>(null);
    @jdk.internal.vm.annotation.Contended
    private volatile Node<T> last = this.first;

    @SuppressWarnings("unchecked")
    @Override
    public <I extends T> int enqueueAll(final I[] items) {
        if ((items == null) || (items.length == 0)) {
            return 0;
        }
        final Node<T> first = new Node<>(checkItem(items[0]));
        Node<T> last = first;
        for (int i = 1; i < items.length; i++) {
            final Node<T> newLast = new Node<>(checkItem(items[i]));
            Node.NextHandle.set(last, newLast);
            last = newLast;
        }
        final Node<T> oldLast = (Node<T>)LastHandle.getAndSetRelease(this, last);
        Node.NextHandle.setOpaque(oldLast, first);
        return items.length;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int enqueueAll(final Iterable<? extends T> items) {
        if (items == null) {
            return 0;
        }
        return this.enqueueRemaining(items.iterator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public int enqueueRemaining(final Iterator<? extends T> iterator) {
        if ((iterator == null) || (!iterator.hasNext())) {
            return 0;
        }
        final Node<T> first = new Node<>(checkItem(iterator.next()));
        int count = 1;
        Node<T> last = first;
        while (iterator.hasNext()) {
            final Node<T> newLast = new Node<>(checkItem(iterator.next()));
            Node.NextHandle.set(last, newLast);
            last = newLast;
            count++;
        }
        final Node<T> oldLast = (Node<T>)LastHandle.getAndSetRelease(this, last);
        Node.NextHandle.setOpaque(oldLast, first);
        return count;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enqueue(final T item) {
        final Node<T> newLast = new Node<>(checkItem(item));
        final Node<T> oldLast = (Node<T>)LastHandle.getAndSetRelease(this, newLast);
        Node.NextHandle.setOpaque(oldLast, newLast);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T dequeue() {
        while (true) {
            final Node<T> first = (Node<T>)FirstHandle.getAcquire(this);
            final Node<T> next = (Node<T>)Node.NextHandle.getOpaque(first);
            if (next == null) {
                return null;
            }
            if (!FirstHandle.weakCompareAndSetRelease(this, first, next)) {
                continue;
            }
            return next.content;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<T> dequeueAll() {
        final Node<T> first = (Node<T>)FirstHandle.getAcquire(this);
        final Node<T> next = (Node<T>)Node.NextHandle.getOpaque(first);
        if (next == null) {
            return this.emptyIterable;
        }
        return (Node.NextHandle.getOpaque(next) == null)
                ? getItem(first, next)
                : getItems(first, next);
    }

    @SuppressWarnings("unchecked")
    private Iterable<T> getItem(Node<T> first, Node<T> next) {
        while (true) {
            if (FirstHandle.weakCompareAndSetRelease(this, first, next)) {
                return new ItemIterable<>(next);
            }
            first = (Node<T>)FirstHandle.getAcquire(this);
            next = (Node<T>)Node.NextHandle.getOpaque(first);
            if (next == null) {
                return this.emptyIterable;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Iterable<T> getItems(Node<T> first, Node<T> next) {
        final Node<T> newLast = new Node<>(null);
        while (true) {
            if (FirstHandle.weakCompareAndSetRelease(this, first, newLast)) {
                final Node<T> oldLast = (Node<T>)LastHandle.getAndSetRelease(this, newLast);
                Node.NextHandle.setOpaque(oldLast, newLast);
                return new ItemsIterable<>(next, newLast);
            }
            first = (Node<T>)FirstHandle.getAcquire(this);
            next = (Node<T>)Node.NextHandle.getOpaque(first);
            if (next == null) {
                return this.emptyIterable;
            }
        }
    }

    private static <I> I checkItem(final I item) {
        if (item == null) {
            throw new NullPointerException("item");
        }
        return item;
    }

    private final static class Node<I> {

        private final static VarHandle NextHandle;

        static {
            try {
                NextHandle = MethodHandles.lookup().findVarHandle(Node.class, "next", Node.class);
            } catch (Exception exc) {
                throw new Error(exc);
            }
        }

        private volatile Node<I> next;

        private I content;

        private Node(final I content) {
            this.content = content;
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

    private final static class ItemsIterable<T> implements Iterable<T> {

        private Node<T> first;
        private Node<T> last;

        private ItemsIterable(final Node<T> first, final Node<T> last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public Iterator<T> iterator() {
            return new ItemsIterator<>(this.first, this.last);
        }
    }

    private final static class ItemsIterator<T> implements Iterator<T> {

        private Node<T> next;
        private Node<T> last;

        private T item = null;

        private ItemsIterator(final Node<T> first, final Node<T> last) {
            this.next = first;
            this.last = last;
            this.item = this.fetchItem();
        }

        @Override
        public boolean hasNext() {
            return (this.item != null);
        }

        @Override
        public T next() {
            if (this.item == null) {
                throw new NoSuchElementException();
            }
            final T currentItem = this.item;
            this.item = this.fetchItem();
            return currentItem;
        }

        private T fetchItem() {
            final Node<T> current = this.next;
            if (current == null) {
                return null;
            }
            this.next = this.fetchNode();
            return current.content;
        }

        @SuppressWarnings("unchecked")
        private Node<T> fetchNode() {
            final Node<T> node = this.next;
            Node<T> result;
            while ((result = (Node<T>)Node.NextHandle.getOpaque(node)) == null) {
                Thread.onSpinWait();
            }
            return (result == this.last) ? null : result;
        }
    }

    private final static class ItemIterable<T> implements Iterable<T> {

        private Node<T> node;

        private ItemIterable(final Node<T> node) {
            this.node = node;
        }

        @Override
        public Iterator<T> iterator() {
            return new ItemIterator<>(this.node);
        }
    }

    private final static class ItemIterator<T> implements Iterator<T> {

        private T item = null;

        private ItemIterator(final Node<T> node) {
            this.item = node.content;
        }

        @Override
        public boolean hasNext() {
            return (this.item != null);
        }

        @Override
        public T next() {
            if (this.item == null) {
                throw new NoSuchElementException();
            }
            final T currentItem = this.item;
            this.item = null;
            return currentItem;
        }
    }
}
