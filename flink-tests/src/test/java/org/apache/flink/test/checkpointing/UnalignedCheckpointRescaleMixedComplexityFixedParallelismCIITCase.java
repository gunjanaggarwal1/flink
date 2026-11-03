/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.VirtualMemory;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/**
 * CI-friendly variant of {@link UnalignedCheckpointRescaleMixedComplexityFixedParallelismITCase}.
 *
 * <p>Uses the same DAG topology, fixed parallelism scenarios, sleeping operators, and two-phase
 * test logic (checkpoint then rescale). Sources run at 5,000 rec/sec each (10,000 total), and the
 * 1 KB memory segment size matches the original test to maximize in-flight buffer pressure.
 */
@ExtendWith(ParameterizedTestExtension.class)
class UnalignedCheckpointRescaleMixedComplexityFixedParallelismCIITCase {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int SLOTS_PER_TASK_MANAGER = 10;
    private static final int MAX_SLOTS = NUM_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;

    /**
     * Short enough that an expired checkpoint fails the job (no tolerable checkpoint failures, no
     * restarts) well before the CI step timeout, so the per-subtask records still get logged.
     */
    private static final Duration CHECKPOINT_TIMEOUT = Duration.ofSeconds(60);

    /** Upper bound for waiting on a checkpoint, in case the job neither checkpoints nor fails. */
    private static final Duration CHECKPOINT_WAIT_TIMEOUT = CHECKPOINT_TIMEOUT.multipliedBy(2);

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    UnalignedCheckpointRescaleMixedComplexityFixedParallelismCIITCase.class);

    /**
     * Records processed by each subtask of the currently running job, keyed by "operator
     * (index/parallelism)". The MiniCluster runs in the test JVM, so the operators can update it
     * directly. It is reset whenever a new job is submitted.
     */
    private static final Map<String, RecordCounter> RECORDS_PER_SUBTASK =
            new ConcurrentHashMap<>();

    @Parameter private Scenario scenario;

    @Parameters(name = "scenario={0}")
    private static List<Scenario> parameters() {
        return Arrays.asList(Scenario.values());
    }

    /**
     * Initial and rescaled parallelisms. "Map" always follows source2 and "Map after forward"
     * always follows coMap, to keep their forward exchanges. Every value must be in [1, MAX_SLOTS].
     */
    private enum Scenario {
        /** Parallelisms observed to fail on the jdk17 CI run. */
        JDK17(new DagParallelism(8, 5, 10, 6), new DagParallelism(2, 4, 4, 1));

        private final DagParallelism initial;
        private final DagParallelism rescaled;

        Scenario(DagParallelism initial, DagParallelism rescaled) {
            this.initial = initial;
            this.rescaled = rescaled;
        }
    }

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            // Must be set at cluster level because StandaloneCompletedCheckpointStore
                            // reads it from the cluster config, not the per-job config. This prevents
                            // the picked checkpoint from being subsumed before the next job restores.
                            .setConfiguration(
                                    new Configuration()
                                            .set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50))
                            .setNumberTaskManagers(NUM_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                            .build());

    @TempDir private File temporaryFolder;

    @TestTemplate
    void testRescaleFromUnalignedCheckpoint(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        // Step 1: Run the job with initial parallelism and take a checkpoint
        JobClient jobClient1 =
                createMixedComplexityDAG(getUnalignedCheckpointEnv(null), scenario.initial);

        CommonTestUtils.waitForJobStatus(jobClient1, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient1.getJobID(), false);
        logParallelism("Job 1", jobClient1.getJobID(), miniCluster);
        String checkpointPath =
                waitForCheckpointWithInflightBuffers("Job 1", jobClient1.getJobID(), miniCluster);
        CommonTestUtils.terminateJob(jobClient1);

        // Step 2: Restore the job with the rescaled parallelism
        JobClient jobClient2 =
                createMixedComplexityDAG(
                        getUnalignedCheckpointEnv(checkpointPath), scenario.rescaled);

        CommonTestUtils.waitForJobStatus(jobClient2, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient2.getJobID(), false);
        logParallelism("Job 2", jobClient2.getJobID(), miniCluster);
        waitForCheckpointWithInflightBuffers("Job 2", jobClient2.getJobID(), miniCluster);
        CommonTestUtils.terminateJob(jobClient2);
    }

    @Test
    void testReproduceCheckpointDeadlock(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        // Phase 1: initial job with safe parallelism — checkpoint completes fine
        JobClient jobClient1 =
                createMixedComplexityDAG(getUnalignedCheckpointEnv(null), 9, 7, 9, 6);
        CommonTestUtils.waitForJobStatus(jobClient1, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient1.getJobID(), false);
        logParallelism("Job 1", jobClient1.getJobID(), miniCluster);
        String checkpointPath =
                waitForCheckpointWithInflightBuffers("Job 1", jobClient1.getJobID(), miniCluster);
        CommonTestUtils.terminateJob(jobClient1);

        // Phase 2: rescaled job with coMapPar=2
        // 10,000 rec/sec total into Co-Map, 2 subtasks → 5,000 rec/sec per subtask
        // Map after forward: FORWARD (aligned checkpoint), sleep(1ms) = 1,000 rec/sec capacity
        // 5x overload → aligned checkpoint barrier stuck in full buffers → checkpoint expires
        JobClient jobClient2 =
                createMixedComplexityDAG(getUnalignedCheckpointEnv(checkpointPath), 1, 5, 2, 1);
        CommonTestUtils.waitForJobStatus(jobClient2, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient2.getJobID(), false);
        logParallelism("Job 2", jobClient2.getJobID(), miniCluster);
        waitForCheckpointWithInflightBuffers("Job 2", jobClient2.getJobID(), miniCluster);
        CommonTestUtils.terminateJob(jobClient2);
    }

    private StreamExecutionEnvironment getUnalignedCheckpointEnv(@Nullable String recoveryPath) {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        conf.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, CHECKPOINT_TIMEOUT);
        // Disable aligned timeout to ensure it works with unaligned checkpoint directly
        conf.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(0));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.toURI().toString());
        conf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("1 kb"));
        if (recoveryPath != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, recoveryPath);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.disableOperatorChaining();
        return env;
    }

    private static JobClient createMixedComplexityDAG(
            StreamExecutionEnvironment env, DagParallelism parallelism) throws Exception {
        return createMixedComplexityDAG(
                env,
                parallelism.source1,
                parallelism.source2,
                parallelism.coMap,
                parallelism.mapAfterKeyBy);
    }

    private static JobClient createMixedComplexityDAG(
            StreamExecutionEnvironment env,
            int source1Par,
            int source2Par,
            int coMapPar,
            int mapKeyByPar)
            throws Exception {
        // Multi-input part
        DataStream<Long> sourceStream1 =
                env.fromSource(
                                createSource("Source 1"),
                                WatermarkStrategy.noWatermarks(),
                                "Source 1")
                        .setParallelism(source1Par);

        DataStream<Long> sourceStream2 =
                env.fromSource(
                                createSource("Source 2"),
                                WatermarkStrategy.noWatermarks(),
                                "Source 2")
                        .setParallelism(source2Par);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        DataStream<Long> forwardedStream =
                sourceStream2.map(new CountingMap("Map", 0)).setParallelism(source2Par);

        DataStream<Long> multiInputMap =
                sourceStream1
                        .rebalance()
                        .connect(forwardedStream.rebalance())
                        .map(new SleepingCoMap())
                        .name("Co-Map")
                        .setParallelism(coMapPar);

        // Multi-output part
        multiInputMap
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(new CountingMap("Map after keyBy", 2)) // ~500 rec/sec per subtask
                .name("Map after keyBy")
                .setParallelism(mapKeyByPar);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        multiInputMap
                .map(new CountingMap("Map after forward", 1))
                .name("Map after forward")
                .setParallelism(multiInputMap.getParallelism());

        // The previous job has terminated by now, so its counts are no longer relevant.
        RECORDS_PER_SUBTASK.clear();
        return env.executeAsync();
    }

    /**
     * Waits until a checkpoint with in-flight buffers completes and returns its path. Unlike {@link
     * CommonTestUtils#waitForCheckpointWithInflightBuffers}, the wait is bounded, and the records
     * per subtask are logged both on success and on failure/timeout.
     */
    private static String waitForCheckpointWithInflightBuffers(
            String jobLabel, JobID jobId, MiniCluster miniCluster) throws Exception {
        final Deadline deadline = Deadline.fromNow(CHECKPOINT_WAIT_TIMEOUT);
        while (true) {
            final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
            final CheckpointStatsSnapshot stats = graph.getCheckpointStatsSnapshot();
            final CompletedCheckpointStats latestCompleted =
                    stats == null ? null : stats.getHistory().getLatestCompletedCheckpoint();
            if (latestCompleted != null && latestCompleted.getPersistedData() > 0) {
                logRecordsPerSubtask(
                        String.format(
                                "%s (%s) completed checkpoint with in-flight buffers",
                                jobLabel, jobId),
                        stats);
                return latestCompleted.getExternalPath();
            }

            if (graph.getState().isGloballyTerminalState()) {
                final String reason =
                        String.format(
                                "%s (%s) terminated in state %s before completing a checkpoint"
                                        + " with in-flight buffers",
                                jobLabel, jobId, graph.getState());
                logRecordsPerSubtask(reason, stats);
                throw graph.getFailureInfo() == null
                        ? new IllegalStateException(reason)
                        : new IllegalStateException(reason, graph.getFailureInfo().getException());
            }

            if (!deadline.hasTimeLeft()) {
                final String reason =
                        String.format(
                                "%s (%s) did not complete a checkpoint with in-flight buffers"
                                        + " within %s",
                                jobLabel, jobId, CHECKPOINT_WAIT_TIMEOUT);
                logRecordsPerSubtask(reason, stats);
                throw new TimeoutException(reason);
            }

            Thread.sleep(100);
        }
    }

    /** Logs one summary line with the deployed parallelism of every operator of the job. */
    private static void logParallelism(String jobLabel, JobID jobId, MiniCluster miniCluster)
            throws Exception {
        final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
        final StringJoiner parallelisms = new StringJoiner(", ");
        for (AccessExecutionJobVertex vertex : graph.getVerticesTopologically()) {
            parallelisms.add(vertex.getName() + "=" + vertex.getParallelism());
        }
        LOG.info("{} parallelism: {}", jobLabel, parallelisms);
        logSystemResources(jobLabel + " running");
    }

    /**
     * Logs CPU, RAM and swap usage of the host, to tell whether a stuck checkpoint correlates with
     * an overloaded CI runner.
     */
    private static void logSystemResources(String context) {
        try {
            final HardwareAbstractionLayer hardware = new SystemInfo().getHardware();
            final CentralProcessor processor = hardware.getProcessor();
            final GlobalMemory memory = hardware.getMemory();
            final VirtualMemory swap = memory.getVirtualMemory();
            final long[] cpuTicks = processor.getSystemCpuLoadTicks();
            Thread.sleep(500);
            final double cpuLoad = processor.getSystemCpuLoadBetweenTicks(cpuTicks);
            final double[] loadAverage = processor.getSystemLoadAverage(3);
            final Runtime jvm = Runtime.getRuntime();
            LOG.info(
                    "System resources ({}): CPU: cores={}, usage={}%,"
                            + " loadAverage(1m/5m/15m)={}/{}/{}"
                            + " | RAM: used={}/{} MiB, available={} MiB, JVM heap used={}/{} MiB"
                            + " | Swap: used={}/{} MiB",
                    context,
                    processor.getLogicalProcessorCount(),
                    String.format("%.1f", cpuLoad * 100),
                    String.format("%.2f", loadAverage[0]),
                    String.format("%.2f", loadAverage[1]),
                    String.format("%.2f", loadAverage[2]),
                    toMiB(memory.getTotal() - memory.getAvailable()),
                    toMiB(memory.getTotal()),
                    toMiB(memory.getAvailable()),
                    toMiB(jvm.totalMemory() - jvm.freeMemory()),
                    toMiB(jvm.maxMemory()),
                    toMiB(swap.getSwapUsed()),
                    toMiB(swap.getSwapTotal()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Could not log system resources ({})", context, e);
        }
    }

    private static long toMiB(long bytes) {
        return bytes >> 20;
    }

    private static void logRecordsPerSubtask(
            String reason, @Nullable CheckpointStatsSnapshot stats) {
        logSystemResources(reason);
        LOG.info(
                "{}. Checkpoints: completed={}, failed={}, inProgress={}. Records per subtask:",
                reason,
                stats == null ? "n/a" : stats.getCounts().getNumberOfCompletedCheckpoints(),
                stats == null ? "n/a" : stats.getCounts().getNumberOfFailedCheckpoints(),
                stats == null ? "n/a" : stats.getCounts().getNumberOfInProgressCheckpoints());
        for (Map.Entry<String, RecordCounter> entry :
                new TreeMap<>(RECORDS_PER_SUBTASK).entrySet()) {
            LOG.info(
                    "  {}: recordsIn={}, recordsOut={}",
                    entry.getKey(),
                    entry.getValue().in.get(),
                    entry.getValue().out.get());
        }
    }

    private static RecordCounter recordCounter(String operator, int subtaskIndex, int parallelism) {
        return RECORDS_PER_SUBTASK.computeIfAbsent(
                String.format("%s (%d/%d)", operator, subtaskIndex + 1, parallelism),
                ignored -> new RecordCounter());
    }

    private static RecordCounter recordCounter(String operator, RuntimeContext context) {
        return recordCounter(
                operator,
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getNumberOfParallelSubtasks());
    }

    private static DataGeneratorSource<Long> createSource(String name) {
        return new DataGeneratorSource<>(
                new CountingGenerator(name),
                new NumberSequenceSource(0, Long.MAX_VALUE - 1) {
                    @Override
                    protected List<NumberSequenceSplit> splitNumberRange(
                            long from, long to, int numSplitsIgnored) {
                        return super.splitNumberRange(from, to, MAX_SLOTS);
                    }
                },
                RateLimiterStrategy.perSecond(5000),
                Types.LONG);
    }

    /** Parallelism of each independently scalable operator in the mixed complexity DAG. */
    private static class DagParallelism {
        private final int source1;
        private final int source2;
        private final int coMap;
        private final int mapAfterKeyBy;

        private DagParallelism(int source1, int source2, int coMap, int mapAfterKeyBy) {
            this.source1 = source1;
            this.source2 = source2;
            this.coMap = coMap;
            this.mapAfterKeyBy = mapAfterKeyBy;
        }
    }

    /** Records counts of a single subtask. */
    private static class RecordCounter {
        private final AtomicLong in = new AtomicLong();
        private final AtomicLong out = new AtomicLong();
    }

    /** Emits the sequence index and counts the generated records per source subtask. */
    private static class CountingGenerator implements GeneratorFunction<Long, Long> {
        private final String name;
        private transient RecordCounter counter;

        private CountingGenerator(String name) {
            this.name = name;
        }

        @Override
        public void open(SourceReaderContext readerContext) {
            counter =
                    recordCounter(
                            name,
                            readerContext.getIndexOfSubtask(),
                            readerContext.currentParallelism());
        }

        @Override
        public Long map(Long index) {
            counter.out.incrementAndGet();
            return index;
        }
    }

    /**
     * A MapFunction that passes values through, optionally sleeping for each element to create
     * backpressure, and counts them per subtask.
     */
    private static class CountingMap extends RichMapFunction<Long, Long> {
        private final String name;
        private final long sleepMillis;
        private transient RecordCounter counter;

        private CountingMap(String name, long sleepMillis) {
            this.name = name;
            this.sleepMillis = sleepMillis;
        }

        @Override
        public void open(OpenContext openContext) {
            counter = recordCounter(name, getRuntimeContext());
        }

        @Override
        public Long map(Long value) throws Exception {
            counter.in.incrementAndGet();
            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
            counter.out.incrementAndGet();
            return value;
        }
    }

    /** A CoMapFunction that sleeps for 1ms for each element and counts them per subtask. */
    private static class SleepingCoMap extends RichCoMapFunction<Long, Long, Long> {
        private transient RecordCounter counter;

        @Override
        public void open(OpenContext openContext) {
            counter = recordCounter("Co-Map", getRuntimeContext());
        }

        @Override
        public Long map1(Long value) throws Exception {
            return sleepAndCount(value);
        }

        @Override
        public Long map2(Long value) throws Exception {
            return sleepAndCount(value);
        }

        private Long sleepAndCount(Long value) throws InterruptedException {
            counter.in.incrementAndGet();
            Thread.sleep(1);
            counter.out.incrementAndGet();
            return value;
        }
    }
}
