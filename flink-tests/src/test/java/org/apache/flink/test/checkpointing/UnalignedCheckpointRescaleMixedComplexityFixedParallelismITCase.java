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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/**
 * Same as the mixed complexity DAG case of {@link
 * UnalignedCheckpointRescaleWithMixedExchangesITCase}, but with manually chosen parallelisms for
 * the initial and the rescaled job instead of random ones, so that a specific rescaling scenario
 * can be reproduced deterministically.
 *
 * <p>Used to reproduce FLINK-40804: a checkpoint after rescaling from an unaligned checkpoint never
 * completes because the barrier is stuck behind the aligned forward exchange between Co-Map and
 * "Map after forward", until the checkpoint expires and the job fails.
 */
@ExtendWith(ParameterizedTestExtension.class)
class UnalignedCheckpointRescaleMixedComplexityFixedParallelismITCase {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int SLOTS_PER_TASK_MANAGER = 10;
    private static final int MAX_SLOTS = NUM_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;

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
        JDK17(new DagParallelism(9, 7, 9, 6), new DagParallelism(1, 5, 2, 1));
        // Parallelisms observed to fail on the hadoop313 CI run:
        // HADOOP313(new DagParallelism(8, 5, 10, 6), new DagParallelism(2, 4, 4, 1)),
        // Same as JDK17, but Co-Map/"Map after forward" keep up with the load. Expected to pass:
        // SAFE_CONTROL(new DagParallelism(9, 7, 9, 6), new DagParallelism(1, 5, 10, 1)),

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
        String checkpointPath =
                CommonTestUtils.waitForCheckpointWithInflightBuffers(
                        jobClient1.getJobID(), miniCluster);
        CommonTestUtils.terminateJob(jobClient1);

        // Step 2: Restore the job with the rescaled parallelism
        JobClient jobClient2 =
                createMixedComplexityDAG(
                        getUnalignedCheckpointEnv(checkpointPath), scenario.rescaled);

        CommonTestUtils.waitForJobStatus(jobClient2, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient2.getJobID(), false);
        CommonTestUtils.waitForCheckpointWithInflightBuffers(jobClient2.getJobID(), miniCluster);
        CommonTestUtils.terminateJob(jobClient2);
    }

    private StreamExecutionEnvironment getUnalignedCheckpointEnv(@Nullable String recoveryPath) {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        // Disable aligned timeout to ensure it works with unaligned checkpoint directly
        conf.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(0));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.toURI().toString());
        conf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        conf.set(CheckpointingOptions.FILE_MERGING_ENABLED, true);
        // Small buffers keep the number of in-flight records that must be consumed during
        // recovery and during aligned checkpoints on forward exchanges low.
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
        // Multi-input part
        DataStream<Long> sourceStream1 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(parallelism.source1);

        DataStream<Long> sourceStream2 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(parallelism.source2);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        DataStream<Long> forwardedStream =
                sourceStream2.map(x -> x).setParallelism(parallelism.source2);

        DataStream<Long> multiInputMap =
                sourceStream1
                        .rebalance()
                        .connect(forwardedStream.rebalance())
                        .map(new SleepingCoMap())
                        .name("Co-Map")
                        .setParallelism(parallelism.coMap);

        // Multi-output part
        multiInputMap
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5); // ~200 rec/sec per subtask
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(parallelism.mapAfterKeyBy);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        multiInputMap
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after forward")
                .setParallelism(parallelism.coMap);

        return env.executeAsync();
    }

    private static DataGeneratorSource<Long> createSource() {
        return new DataGeneratorSource<>(
                index -> index,
                new NumberSequenceSource(0, Long.MAX_VALUE - 1) {
                    @Override
                    protected List<NumberSequenceSplit> splitNumberRange(
                            long from, long to, int numSplitsIgnored) {
                        return super.splitNumberRange(from, to, MAX_SLOTS);
                    }
                },
                RateLimiterStrategy.perSecond(
                        Integer.parseInt(
                                System.getProperties().getProperty("source.rate", "5000"))),
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

    /** A simple CoMapFunction that sleeps for 1ms for each element. */
    private static class SleepingCoMap implements CoMapFunction<Long, Long, Long> {
        @Override
        public Long map1(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }

        @Override
        public Long map2(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }
    }
}
