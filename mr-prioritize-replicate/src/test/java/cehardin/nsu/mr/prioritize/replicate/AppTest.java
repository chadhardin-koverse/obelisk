package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.base.Functions.forMap;
import static java.lang.String.format;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert.*;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void test1() throws Exception {
        final Variables.Bandwidth diskBandwidth = new Variables.Bandwidth(100000000L, 1, TimeUnit.SECONDS);
        final Variables.Bandwidth rackNetworkBandwidth = new Variables.Bandwidth(1000000000L / 8, 1, TimeUnit.SECONDS);
        final Variables.Bandwidth clusterNetworkBandwidth = new Variables.Bandwidth(10000000000L / 8, 1, TimeUnit.SECONDS);
        final int blockSize = 128 * 1024 * 1024;
        final int numNodes = 1024;
        final int maxConcurrentTasks = numNodes / 8;
        final int maxTasksPerNode = 2;
        final RackId rack1 = new RackId("r1");
        final RackId rack2 = new RackId("r2");
        final NodeId node11 = new NodeId("r1-n1");
        final NodeId node12 = new NodeId("r1-n2");
        final NodeId node21 = new NodeId("r2-n1");
        final NodeId node22 = new NodeId("r2-n2");
        final DataBlockId dataBlock1 = new DataBlockId("db1");
        final DataBlockId dataBlock2 = new DataBlockId("db2");
        final DataBlockId dataBlock3 = new DataBlockId("db3");
        final DataBlockId dataBlock4 = new DataBlockId("db4");
        final DataBlockId dataBlock5 = new DataBlockId("db5");
        final DataBlockId dataBlock6 = new DataBlockId("db6");
        final DataBlockId dataBlock7 = new DataBlockId("db7");
        final DataBlockId dataBlock8 = new DataBlockId("db8");
        final TaskId task1 = new TaskId("t1");
        final TaskId task2 = new TaskId("t2");
        final TaskId task3 = new TaskId("t3");
        final Set<RackId> rackIds = newHashSet(rack1, rack2);
        final Set<NodeId> nodeIds = newHashSet(node11, node12, node21, node22);
        final Set<DataBlockId> dataBlockIds = newHashSet(dataBlock1, dataBlock2, dataBlock3, dataBlock4, dataBlock5, dataBlock6, dataBlock7, dataBlock8);
        final SortedSet<Variables.NodeFailure> nodeFailures = newTreeSet(newArrayList(new Variables.NodeFailure(node11, 30, TimeUnit.SECONDS)));
        final Set<TaskId> taskIds = newHashSet(task1, task2, task3);
        final Map<TaskId, DataBlockId> taskIdToDataBlockId;
        final Map<NodeId, RackId> nodeIdToRackId;
        final Map<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds;
        final TaskNodeAllocator taskNodeAllocator = new StandardTaskNodeAllocator();
        final ReplicateTaskScheduler replicateTaskScheduler = new StandardReplicateTaskScheduler();
        final Variables.MapReduceJob mapReduceJob;
        final Variables variables;
        final Simulator simulator;
        final ExecutorService executorService = Executors.newFixedThreadPool(16);
        final double time;
        final SortedMap<Integer, Integer> counts;
        
        nodeIdToRackId = newHashMap();

        nodeIdToRackId.put(node11, rack1);
        nodeIdToRackId.put(node12, rack1);
        nodeIdToRackId.put(node21, rack2);
        nodeIdToRackId.put(node22, rack2);

        dataBlockIdToNodeIds = newHashMap();
        dataBlockIdToNodeIds.put(dataBlock1, newHashSet(node11, node21, node22));
        dataBlockIdToNodeIds.put(dataBlock2, newHashSet(node12, node22, node21));
        dataBlockIdToNodeIds.put(dataBlock3, newHashSet(node11, node21, node22));
        dataBlockIdToNodeIds.put(dataBlock4, newHashSet(node12, node22, node21));
        dataBlockIdToNodeIds.put(dataBlock5, newHashSet(node11, node21, node12));
        dataBlockIdToNodeIds.put(dataBlock6, newHashSet(node12, node22, node11));
        dataBlockIdToNodeIds.put(dataBlock7, newHashSet(node11, node21, node12));
        dataBlockIdToNodeIds.put(dataBlock8, newHashSet(node12, node22, node11));

        taskIdToDataBlockId = newHashMap();
        taskIdToDataBlockId.put(task1, dataBlock1);
        taskIdToDataBlockId.put(task2, dataBlock2);
        taskIdToDataBlockId.put(task3, dataBlock3);

        mapReduceJob = new Variables.MapReduceJob(
                10L,
                TimeUnit.SECONDS,
                taskIds,
                forMap(taskIdToDataBlockId));

        variables = new Variables(
                diskBandwidth,
                rackNetworkBandwidth,
                clusterNetworkBandwidth,
                blockSize,
                maxConcurrentTasks,
                maxTasksPerNode,
                nodeFailures,
                rackIds,
                nodeIds,
                dataBlockIds,
                forMap(nodeIdToRackId),
                forMap(dataBlockIdToNodeIds),
                taskNodeAllocator,
                replicateTaskScheduler,
                mapReduceJob);

        simulator = new Simulator(variables, executorService);

        time = simulator.call();
        
        System.out.println(format("Time elapsed", time));
        
        counts = transformValues(simulator.getCluster().getReplicationCounts(), new Function<Set<DataBlockId>, Integer>() {
            @Override
            public Integer apply(final Set<DataBlockId> dataBlocks) {
                return dataBlocks.size();
            }
        });
        System.out.println("BLOCK COUNT\t\t\tNODE COUNT");
        for(final Map.Entry<Integer, Integer> countEntry : counts.entrySet()) {
            final int blockCount = countEntry.getKey();
            final int nodeCount = countEntry.getValue();
            System.out.println(format("%s\t\t\t%s", blockCount, nodeCount));
            
        }
    }
}
