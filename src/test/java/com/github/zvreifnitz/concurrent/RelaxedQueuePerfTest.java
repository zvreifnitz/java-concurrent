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

import com.github.zvreifnitz.concurrent.impl.LinkedRelaxedQueue;
import com.github.zvreifnitz.concurrent.impl.LinkedRelaxedQueueWeak;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 20, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 20, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-XX:-RestrictContended"})
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class RelaxedQueuePerfTest {

    private final static int NumOfPingPongs = 10_000_000;

    @Param({"1",/* "2", "4",*/ "8"})
    private int numOfThreads;
    @Param({"0", "1", /*"10", "100",*/ "1000"})
    private int batchSize;
    private int numOfBatches;

    private LinkedRelaxedQueue<Object> strongQueue_Req;
    private LinkedRelaxedQueue<Object> strongQueue_Resp;
    private LinkedRelaxedQueueWeak<Object> weakQueue_Req;
    private LinkedRelaxedQueueWeak<Object> weakQueue_Resp;

    public static void main(final String[] args) {
        final RelaxedQueuePerfTest test = new RelaxedQueuePerfTest();
        test.numOfThreads = 8;
        test.batchSize = 1;
        test.init();
        final long r1 = test.strongQueue();
        System.out.println("strongQueue: " + (r1 / 1_000_000.0));
        final long r2 = test.weakQueue();
        System.out.println("  weakQueue: " + (r2 / 1_000_000.0));
    }

    @Setup
    public void init() {
        this.numOfBatches = ((this.batchSize > 0) ? (NumOfPingPongs / (this.batchSize * this.numOfThreads)) : NumOfPingPongs);
        this.strongQueue_Req = new LinkedRelaxedQueue<>();
        this.strongQueue_Resp = new LinkedRelaxedQueue<>();
        this.weakQueue_Req = new LinkedRelaxedQueueWeak<>();
        this.weakQueue_Resp = new LinkedRelaxedQueueWeak<>();
    }

    @Benchmark
    public long strongQueue() {
        return TestScenario.exec(this.strongQueue_Req, this.strongQueue_Resp,
                this.numOfThreads, this.numOfBatches, this.batchSize);
    }

    @Benchmark
    public long weakQueue() {
        return TestScenario.exec(this.weakQueue_Req, this.weakQueue_Resp,
                this.numOfThreads, this.numOfBatches, this.batchSize);
    }
}
