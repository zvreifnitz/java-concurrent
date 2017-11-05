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
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 20, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 20, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-XX:-RestrictContended"})
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ComparisonPerfTest {

    private final static int NumOfPingPongs = 10_000_000;

    @Param({"1",/* "2", "4",*/ "8"})
    private int numOfThreads;
    @Param({"0", "1", /*"10", "100",*/ "1000"})
    private int batchSize;
    private int numOfBatches;

    private JavaConcurrentLinkedQueue<Object> javaConcurrentLinkedQueue_Req;
    private JavaConcurrentLinkedQueue<Object> javaConcurrentLinkedQueue_Resp;
    private RingBufferRelaxedQueue<Object> ringBufferRelaxedQueue_Req;
    private RingBufferRelaxedQueue<Object> ringBufferRelaxedQueue_Resp;
    private LinkedRelaxedQueue<Object> linkedRelaxedQueue_Req;
    private LinkedRelaxedQueue<Object> linkedRelaxedQueue_Resp;
    private JCToolsMpmcArrayQueue<Object> jcToolsMpmcArrayQueue_Req;
    private JCToolsMpmcArrayQueue<Object> jcToolsMpmcArrayQueue_Resp;

    public static void main(final String[] args) {
        final ComparisonPerfTest test = new ComparisonPerfTest();
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
    }

    @Setup
    public void init() {
        this.numOfBatches = ((this.batchSize > 0) ? (NumOfPingPongs / (this.batchSize * this.numOfThreads)) : NumOfPingPongs);
        this.javaConcurrentLinkedQueue_Req = new JavaConcurrentLinkedQueue<>();
        this.javaConcurrentLinkedQueue_Resp = new JavaConcurrentLinkedQueue<>();
        this.ringBufferRelaxedQueue_Req = new RingBufferRelaxedQueue<>(Object.class, NumOfPingPongs);
        this.ringBufferRelaxedQueue_Resp = new RingBufferRelaxedQueue<>(Object.class, NumOfPingPongs);
        this.linkedRelaxedQueue_Req = new LinkedRelaxedQueue<>();
        this.linkedRelaxedQueue_Resp = new LinkedRelaxedQueue<>();
        this.jcToolsMpmcArrayQueue_Req = new JCToolsMpmcArrayQueue<>(NumOfPingPongs);
        this.jcToolsMpmcArrayQueue_Resp = new JCToolsMpmcArrayQueue<>(NumOfPingPongs);
    }

    @Benchmark
    public long java_ConcurrentLinkedQueue() {
        return TestScenario.exec(this.javaConcurrentLinkedQueue_Req, this.javaConcurrentLinkedQueue_Resp,
                this.numOfThreads, this.numOfBatches, this.batchSize);
    }

    //@Benchmark
    public long custom_RingBufferRelaxedQueue() {
        return TestScenario.exec(this.ringBufferRelaxedQueue_Req, this.ringBufferRelaxedQueue_Resp,
                this.numOfThreads, this.numOfBatches, this.batchSize);
    }

    @Benchmark
    public long custom_LinkedRelaxedQueue() {
        return TestScenario.exec(this.linkedRelaxedQueue_Req, this.linkedRelaxedQueue_Resp,
                this.numOfThreads, this.numOfBatches, this.batchSize);
    }

    @Benchmark
    public long jcTools_MpmcArrayQueue() {
        return TestScenario.exec(this.jcToolsMpmcArrayQueue_Req, this.jcToolsMpmcArrayQueue_Resp,
                this.numOfThreads, this.numOfBatches, this.batchSize);
    }
}
