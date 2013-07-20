/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Multimaps.newMultimap;
import static com.google.common.collect.Maps.transformValues;
import static java.lang.String.format;
import static com.google.common.base.Functions.forMap;
import static cehardin.nsu.mr.prioritize.replicate.Util.pickRandom;
import static cehardin.nsu.mr.prioritize.replicate.Util.pickRandomPercentage;

import cehardin.nsu.mr.prioritize.replicate.Variables.Bandwidth;
import cehardin.nsu.mr.prioritize.replicate.Variables.MapReduceJob;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.ClusterBuilder;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cehar_000
 */
public class App implements Runnable {
    private final Cluster cluster;
    
    public App(Variables variables) {
        final ClusterBuilder clusterBuilder = new ClusterBuilder();
        
        cluster = clusterBuilder.buildCluster(variables);
    }
    
    
    public static void main(String[] args) {
        final Random random = new Random(0);
        final Bandwidth diskBadwidth;
        final Bandwidth rackBandwidth;
        final Bandwidth clusterBandwidth;
        final int blockSize;
        final int numNodes;
        final int maxConcurrentTasks;
        final int maxTasksPerNode;
        final double nodePercentageFailed;
        final SortedSet<Variables.NodeFailure> nodeFailures;
        final int numRacks;
        final int nodesPerRack;
        final int numDataBlocks;
        final Set<RackId> rackIds;
        final Set<NodeId> nodeIds;
        final Set<DataBlockId> dataBlockIds;
        final Map<NodeId, RackId> nodeToRack;
        final Multimap<RackId, NodeId> rackToNodes;
        final Multimap<DataBlockId, NodeId> dataBlockToNodes;
        final Function<NodeId, RackId> nodeToRackFunction;
        final Function<DataBlockId, Set<NodeId>> dataBlockToNodesFunction;
        final TaskNodeAllocator taskNodeAllocator;
        final ReplicateTaskScheduler replicateTaskScheduler;
        final int numTasks;
        final Set<TaskId> taskIds;
        final Map<TaskId, DataBlockId> taskToDataBlock;
        final MapReduceJob mapReduceJob;
        
        final Variables variables;
        
        diskBadwidth = new Bandwidth(100_000_000L, 1, TimeUnit.SECONDS);
        rackBandwidth = new Bandwidth(1_000_000_000L / 8, 1, TimeUnit.SECONDS);
        clusterBandwidth = new Bandwidth(10_000_000_000L / 8, 1, TimeUnit.SECONDS);
        blockSize = 128 * 1024 * 1024;
        numNodes = 1024;
        numRacks = numNodes / 16;
        numDataBlocks = numNodes * 2;
        nodesPerRack = numNodes / numRacks;
        maxConcurrentTasks = numNodes / 8;
        maxTasksPerNode = 2;
        rackIds = newHashSet();
        nodeIds = newHashSet();
        rackToNodes = HashMultimap.create();
        dataBlockToNodes = HashMultimap.create();
        nodeToRack = newHashMap();
        dataBlockIds = newHashSet();
        nodePercentageFailed = 0.25;
        taskNodeAllocator = new StandardTaskNodeAllocator();
        replicateTaskScheduler = new StandardReplicateTaskScheduler();
        numTasks = numNodes * 2;
        
        taskIds = newHashSet();
        for(int i=0; i < numTasks; i++) {
            final TaskId taskId = new TaskId(format("t-%s", i));
            taskIds.add(taskId);
        }
        
        taskToDataBlock = newHashMap();
        for(final TaskId taskId : taskIds) {
            final DataBlockId dataBlockId = pickRandom(random, dataBlockIds);
            taskToDataBlock.put(taskId, dataBlockId);
        }
        
        mapReduceJob = new MapReduceJob(10, TimeUnit.SECONDS, taskIds, forMap(taskToDataBlock));
        for(int r=1; r <= numRacks; r++) {
            final RackId rackId = new RackId(format("r-%s",1));
            rackIds.add(rackId);
            for(int n=1; n <= nodesPerRack; n++) {
                final NodeId nodeId = new NodeId(format("r-%s-n-%s", r, n));
                nodeToRack.put(nodeId, rackId);
                rackToNodes.put(rackId, nodeId);
                nodeIds.add(nodeId);
            }
        }
        
//        nodeFailures = newTreeSet(pickRandomPercentage(random, nodeIds, nodePercentageFailed));
        nodeFailures = newTreeSet();
        
        nodeToRackFunction = forMap(nodeToRack);
        
        for(int d=1; d <= numDataBlocks; d++) {
            final DataBlockId dataBlockId = new DataBlockId(format("d-%s", d));
            dataBlockIds.add(dataBlockId);
        }
        
        for(final DataBlockId dataBlockId : dataBlockIds) {
            final RackId rack1 = pickRandom(random, rackIds);
            final RackId rack2 = pickRandom(random, rackIds, rack1);
            final NodeId node1 = pickRandom(random, rackToNodes.get(rack1));
            final NodeId node2 = pickRandom(random, rackToNodes.get(rack1), node1);
            final NodeId node3 = pickRandom(random, rackToNodes.get(rack2));
            
            dataBlockToNodes.put(dataBlockId, node1);
            dataBlockToNodes.put(dataBlockId, node2);
            dataBlockToNodes.put(dataBlockId, node3);
        }
        
        dataBlockToNodesFunction = forMap(transformValues(dataBlockToNodes.asMap(), new Function<Collection<NodeId>, Set<NodeId>>() {
            @Override
            public Set<NodeId> apply(Collection<NodeId> nodeIds) {
                return newHashSet(nodeIds);
            }            
        }));
        
        variables = new Variables(
                diskBadwidth, 
                rackBandwidth, 
                clusterBandwidth, 
                blockSize, 
                maxConcurrentTasks, 
                maxTasksPerNode, 
                nodeFailures, 
                rackIds, 
                nodeIds, 
                dataBlockIds, 
                nodeToRackFunction, 
                dataBlockToNodesFunction, 
                taskNodeAllocator, 
                replicateTaskScheduler, 
                mapReduceJob);
        
        final Simulator simulator = new Simulator(variables, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
        
        simulator.call();
    }
}

