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

public final class LinkedRelaxedQueue<T> implements RelaxedQueue<T> {

    private static final VarHandle TailHandle;
    private static final VarHandle HeadHandle;

    static {
        try {
            final MethodHandles.Lookup l = MethodHandles.lookup();
            TailHandle = l.findVarHandle(LinkedRelaxedQueue.class, "tail", Node.class);
            HeadHandle = l.findVarHandle(LinkedRelaxedQueue.class, "head", Node.class);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    private final Iterable<T> emptyIterable = new EmptyIterable<>();

    @jdk.internal.vm.annotation.Contended
    private volatile Node<T> head = new Node<>(null);
    @jdk.internal.vm.annotation.Contended
    private volatile Node<T> tail = this.head;

    @SuppressWarnings("unchecked")
    @Override
    public <I extends T> int enqueueAll(final I[] items) {
        if ((items == null) || (items.length == 0)) {
            return 0;
        }
        final Node<T> head = new Node<>(checkItem(items[0]));
        Node<T> tail = head;
        for (int i = 1; i < items.length; i++) {
            final Node<T> newTail = new Node<>(checkItem(items[i]));
            Node.NextHandle.set(tail, newTail);
            tail = newTail;
        }
        final Node<T> oldTail = (Node<T>)TailHandle.getAndSetRelease(this, tail);
        Node.NextHandle.setOpaque(oldTail, head);
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
        final Node<T> head = new Node<>(checkItem(iterator.next()));
        int count = 1;
        Node<T> tail = head;
        while (iterator.hasNext()) {
            final Node<T> newTail = new Node<>(checkItem(iterator.next()));
            Node.NextHandle.set(tail, newTail);
            tail = newTail;
            count++;
        }
        final Node<T> oldTail = (Node<T>)TailHandle.getAndSetRelease(this, tail);
        Node.NextHandle.setOpaque(oldTail, head);
        return count;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enqueue(final T item) {
        final Node<T> newTail = new Node<>(checkItem(item));
        final Node<T> oldTail = (Node<T>)TailHandle.getAndSetRelease(this, newTail);
        Node.NextHandle.setOpaque(oldTail, newTail);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T dequeue() {
        while (true) {
            final Node<T> currentHead = (Node<T>)HeadHandle.getAcquire(this);
            final Node<T> nextHead = (Node<T>)Node.NextHandle.getOpaque(currentHead);
            if (nextHead == null) {
                return null;
            }
            if (!HeadHandle.weakCompareAndSetRelease(this, currentHead, nextHead)) {
                continue;
            }
            return nextHead.content;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<T> dequeueAll() {
        final Node<T> currentHead = (Node<T>)HeadHandle.getAcquire(this);
        final Node<T> nextHead = (Node<T>)Node.NextHandle.getOpaque(currentHead);
        if (nextHead == null) {
            return this.emptyIterable;
        }
        return (Node.NextHandle.getOpaque(nextHead) == null)
                ? getItem(currentHead, nextHead)
                : getItems(currentHead, nextHead);
    }

    @SuppressWarnings("unchecked")
    private Iterable<T> getItem(Node<T> currentHead, Node<T> nextHead) {
        while (true) {
            if (HeadHandle.weakCompareAndSetRelease(this, currentHead, nextHead)) {
                return new ItemIterable<>(nextHead);
            }
            currentHead = (Node<T>)HeadHandle.getAcquire(this);
            nextHead = (Node<T>)Node.NextHandle.getOpaque(currentHead);
            if (nextHead == null) {
                return this.emptyIterable;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Iterable<T> getItems(Node<T> currentHead, Node<T> nextHead) {
        final Node<T> newTail = new Node<>(null);
        while (true) {
            if (HeadHandle.weakCompareAndSetRelease(this, currentHead, newTail)) {
                final Node<T> oldTail = (Node<T>)TailHandle.getAndSetRelease(this, newTail);
                Node.NextHandle.setOpaque(oldTail, newTail);
                return new ItemsIterable<>(nextHead, newTail);
            }
            currentHead = (Node<T>)HeadHandle.getAcquire(this);
            nextHead = (Node<T>)Node.NextHandle.getOpaque(currentHead);
            if (nextHead == null) {
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

        private Node<T> startNode;
        private Node<T> endNode;

        private ItemsIterable(final Node<T> startNode, final Node<T> endNode) {
            this.startNode = startNode;
            this.endNode = endNode;
        }

        @Override
        public Iterator<T> iterator() {
            return new ItemsIterator<>(this.startNode, this.endNode);
        }
    }

    private final static class ItemsIterator<T> implements Iterator<T> {

        private Node<T> nextNode;
        private Node<T> endNode;

        private T nextItem = null;

        private ItemsIterator(final Node<T> startNode, final Node<T> endNode) {
            this.nextNode = startNode;
            this.endNode = endNode;
            this.nextItem = this.fetchNextItem();
        }

        @Override
        public boolean hasNext() {
            return (this.nextItem != null);
        }

        @Override
        public T next() {
            if (this.nextItem == null) {
                throw new NoSuchElementException();
            }
            final T currentItem = this.nextItem;
            this.nextItem = this.fetchNextItem();
            return currentItem;
        }

        private T fetchNextItem() {
            final Node<T> currentNode = this.nextNode;
            if (currentNode == null) {
                return null;
            }
            this.nextNode = this.fetchNextNode();
            return currentNode.content;
        }

        @SuppressWarnings("unchecked")
        private Node<T> fetchNextNode() {
            final Node<T> node = this.nextNode;
            Node<T> result;
            while ((result = (Node<T>)Node.NextHandle.getOpaque(node)) == null) {
                Thread.onSpinWait();
            }
            return (result == this.endNode) ? null : result;
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

        private T nextItem = null;

        private ItemIterator(final Node<T> node) {
            this.nextItem = node.content;
        }

        @Override
        public boolean hasNext() {
            return (this.nextItem != null);
        }

        @Override
        public T next() {
            if (this.nextItem == null) {
                throw new NoSuchElementException();
            }
            final T currentItem = this.nextItem;
            this.nextItem = null;
            return currentItem;
        }
    }
}
