/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import static cehardin.nsu.mr.prioritize.replicate.Util.pickRandom;
import static com.google.common.collect.Sets.intersection;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import static com.google.common.base.Functions.forMap;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import com.google.common.collect.Multimap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cehar_000
 */
public class VariablesFactory implements Supplier<Variables>{

    private final Random random;
    private final Variables.Bandwidth diskBadwidth;
    private final Variables.Bandwidth rackBandwidth;
    private final Variables.Bandwidth clusterBandwidth;
    private final int blockSize;
    private final int numNodes;
    private final int numRacks;
    private final int numDataBlocks;
    private final int maxConcurrentTasks;
    private final int maxTasksPerNode;
    private final double nodePercentageFailed;
    private final long mapReduceStartTime; 
    private final int numTasks;

    public VariablesFactory(
            Random random, 
            Variables.Bandwidth diskBadwidth, 
            Variables.Bandwidth rackBandwidth, 
            Variables.Bandwidth clusterBandwidth, 
            int blockSize, 
            int numNodes, 
            int numRacks, 
            int numDataBlocks, 
            int maxConcurrentTasks, 
            int maxTasksPerNode, 
            double nodePercentageFailed,
            long mapReduceStartTime,
            int numTasks) {
        this.random = random;
        this.diskBadwidth = diskBadwidth;
        this.rackBandwidth = rackBandwidth;
        this.clusterBandwidth = clusterBandwidth;
        this.blockSize = blockSize;
        this.numNodes = numNodes;
        this.numRacks = numRacks;
        this.numDataBlocks = numDataBlocks;
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.maxTasksPerNode = maxTasksPerNode;
        this.nodePercentageFailed = nodePercentageFailed;
        this.mapReduceStartTime = mapReduceStartTime;
        this.numTasks = numTasks;
    }

    @Override
    public Variables get() {
        final SortedSet<Variables.NodeFailure> nodeFailures;
        final int nodesPerRack = numNodes / numRacks;
        final Set<RackId> rackIds = newHashSet();
        final Set<NodeId> nodeIds = newHashSet();
        final List<DataBlockId> dataBlockIds = newArrayList();
        final Map<NodeId, RackId> nodeToRack = newHashMap();
        final Multimap<RackId, NodeId> rackToNodes = HashMultimap.create();
        final Multimap<DataBlockId, NodeId> dataBlockToNodes = HashMultimap.create();
        final Map<NodeId, Set<DataBlockId>> nodeToDataBlocks = new HashMap<>();
        final Function<NodeId, RackId> nodeToRackFunction;
        final Function<DataBlockId, Set<NodeId>> dataBlockToNodesFunction;
        final Function<NodeId, Set<DataBlockId>> nodeToDataBlocksFunction;
        final TaskNodeAllocator taskNodeAllocator = new StandardTaskNodeAllocator();
        final Set<TaskId> taskIds = newHashSet();
        final Map<TaskId, DataBlockId> taskToDataBlock = newHashMap();
        final Variables.MapReduceJob mapReduceJob;
        
        //create racks and nodes
        System.out.printf("Creating %s racks with %s nodes per rack%n", numRacks, nodesPerRack);
        for(int r=1; r <= numRacks; r++) {
            final RackId rackId = new RackId(format("r-%s",r));
            rackIds.add(rackId);
            for(int n=1; n <= nodesPerRack; n++) {
                final NodeId nodeId = new NodeId(format("r-%s-n-%s", r, n));
                nodeToRack.put(nodeId, rackId);
                rackToNodes.put(rackId, nodeId);
                nodeIds.add(nodeId);
            }
        }
        
        //create data blocks
        System.out.printf("Creating %s data blocks%n", numDataBlocks);
        for(int d=1; d <= numDataBlocks; d++) {
            final DataBlockId dataBlockId = new DataBlockId(format("d-%s", d));
            dataBlockIds.add(dataBlockId);
        }
        
        for(final NodeId nodeId : nodeIds) {
            nodeToDataBlocks.put(nodeId, new HashSet<DataBlockId>());
        }
        
        //assign data blocks to nodes
        System.out.printf("Assigning data blocks to nodes%n");
        for(final DataBlockId dataBlockId : dataBlockIds) {
            final RackId rack1 = pickRandom(random, rackIds);
            final RackId rack2 = pickRandom(random, rackIds, rack1);
            final NodeId node1 = pickRandom(random, rackToNodes.get(rack1));
            final NodeId node2 = pickRandom(random, rackToNodes.get(rack1), node1);
            final NodeId node3 = pickRandom(random, rackToNodes.get(rack2));
            
            dataBlockToNodes.put(dataBlockId, node1);
            dataBlockToNodes.put(dataBlockId, node2);
            dataBlockToNodes.put(dataBlockId, node3);
            nodeToDataBlocks.get(node1).add(dataBlockId);
            nodeToDataBlocks.get(node2).add(dataBlockId);
            nodeToDataBlocks.get(node3).add(dataBlockId);
        }
        
        //create the MR tasks
        System.out.printf("Creating %s MR tasks%n", numTasks);
        for(int i=0; i < numTasks; i++) {
            final TaskId taskId = new TaskId(format("t-%s", i));
            taskIds.add(taskId);
        }
        
        //assign data blocks to the MR task
        {
            int percentDone = 0;
            System.out.printf("Assigning data blocks to MR tasks%n");
            System.out.printf("%s%%, ", percentDone);
            
            for(final TaskId taskId : taskIds) {
                final DataBlockId dataBlockId = pickRandom(random, dataBlockIds);
                final int newPercentDone;
                    
                taskToDataBlock.put(taskId, dataBlockId);
                newPercentDone = (taskToDataBlock.size() * 100 )/ taskIds.size();
                
                if(newPercentDone != percentDone) {
                    percentDone = newPercentDone;
                    System.out.printf("%s%%, ", percentDone);
                }
            }
            
            System.out.printf("%n");
        }
        
        mapReduceJob = new Variables.MapReduceJob(mapReduceStartTime, TimeUnit.MILLISECONDS, taskIds, forMap(taskToDataBlock));
        
        nodeFailures = newTreeSet();
        
        System.out.printf("Creating failures (%s%%)%n", nodePercentageFailed * 100);
        {
            final Map<DataBlockId, Set<NodeId>> workingDataBlockToNodes = newHashMap();
            final Set<DataBlockId> criticalDataBlocks = newHashSet();
            final Set<NodeId> criticalNodes = newHashSet();
            final Set<NodeId> uncriticalNodes = newHashSet();
            final Set<NodeId> killedNodes = newHashSet();
            final int numFailedNodes = (int)((double)nodeIds.size() * nodePercentageFailed);
            final Map<DataBlockId, Integer> afterFailureBlockCount = newHashMap();
            int numCriticalNodesKilled = 0;
            int numUnCriticalNodesKilled = 0;
            
            for(final DataBlockId dataBlockId : dataBlockToNodes.keySet()) {
                workingDataBlockToNodes.put(dataBlockId, new HashSet<NodeId>());
                workingDataBlockToNodes.get(dataBlockId).addAll(dataBlockToNodes.get(dataBlockId));
            }
            
            for(final TaskId taskId : mapReduceJob.getTaskIds()) {
                criticalDataBlocks.add(mapReduceJob.getTaskIdToDataBlockId().apply(taskId));
            }
            
            for(final DataBlockId dataBlockId : criticalDataBlocks) {
                criticalNodes.addAll(dataBlockToNodes.get(dataBlockId));
            }
            
            for(final NodeId nodeId : nodeIds) {
                if(!criticalNodes.contains(nodeId)) {
                    uncriticalNodes.add(nodeId);
                }
            }
            
            //Create the failures, heavilly slanted towards nodes that
            //are critical, as in, they contain data blocks that are used
            //by the map reduce job
            while(killedNodes.size() < numFailedNodes) {
                final Iterator<NodeId> criticalNodesIterator = criticalNodes.iterator();
                final Iterator<NodeId> uncriticalNodesIterator = uncriticalNodes.iterator();
                NodeId killedNode = null;
                
                while(criticalNodesIterator.hasNext()) {
                    final NodeId criticalNode = criticalNodesIterator.next();
                    boolean canKillNode = true;
                    for(final DataBlockId dataBlockId : nodeToDataBlocks.get(criticalNode)) {
                        if(criticalDataBlocks.contains(dataBlockId)) {
                            final int count = workingDataBlockToNodes.get(dataBlockId).size();
                            
                            if(count == 1) {
                                canKillNode = false;
                                break;
                            }
                        }
                    }
                    
                    if(canKillNode) {
                        killedNode = criticalNode;
//                        System.out.printf("Failing CRITICAL node %s%n", killedNode);
                        numCriticalNodesKilled++;
                        criticalNodesIterator.remove();
                        break;
                    }
                    else {
                        criticalNodesIterator.remove();
                    }
                }
                
                if(killedNode == null) {
                    while(uncriticalNodesIterator.hasNext()) {
                        final NodeId uncriticalNode = uncriticalNodesIterator.next();
                        killedNode = uncriticalNode;
//                        System.out.printf("Failing uncritical node %s%n", killedNode);
                        numUnCriticalNodesKilled++;
                        uncriticalNodesIterator.remove();
                        break;
                    }
                }
                
                if(killedNode == null) {
                    throw new IllegalStateException("Could not kill enough nodes for MR job to still complete");
                }
                else {
                    killedNodes.add(killedNode);
                    for(final DataBlockId workingDataBlockId : workingDataBlockToNodes.keySet()) {
                        workingDataBlockToNodes.get(workingDataBlockId).remove(killedNode);
                    }
                }
            }
            
            //create the node failues
            for(final NodeId killedNode : killedNodes) {
                final Variables.NodeFailure nodeFailure = new Variables.NodeFailure(killedNode, 0, TimeUnit.MINUTES);
                
                nodeFailures.add(nodeFailure);
            }
            
            //determine the block counts after failure
            for(final DataBlockId dataBlockId : dataBlockIds) {
                afterFailureBlockCount.put(dataBlockId, 0);
                
                for(final NodeId nodeId : dataBlockToNodes.get(dataBlockId)) {
                    if(!killedNodes.contains(nodeId)) {
                        afterFailureBlockCount.put(dataBlockId, afterFailureBlockCount.get(dataBlockId) + 1);
                    }
                }
            }
            
            //sanity check
            for(final TaskId taskId : mapReduceJob.getTaskIds()) {
                final DataBlockId dataBlockId = mapReduceJob.getTaskIdToDataBlockId().apply(taskId);
                final int count = afterFailureBlockCount.get(dataBlockId);
                
                if(count == 0) {
                    throw new IllegalStateException(format("Cannot continue because data block %s has been wiped out", dataBlockId));
                }
            }
            
            System.out.printf("%nSet up %,d node failures.  %,d are critical and %,d are not critical%n", nodeFailures.size(), numCriticalNodesKilled, numUnCriticalNodesKilled);
        }
        
        
        
        
        nodeToRackFunction = forMap(nodeToRack);
        
        dataBlockToNodesFunction = forMap(transformValues(dataBlockToNodes.asMap(), new Function<Collection<NodeId>, Set<NodeId>>() {
            @Override
            public Set<NodeId> apply(Collection<NodeId> nodeIds) {
                return newHashSet(nodeIds);
            }            
        }));
        
        nodeToDataBlocksFunction = forMap(nodeToDataBlocks);
        
        return new Variables(
                diskBadwidth, 
                rackBandwidth, 
                clusterBandwidth, 
                blockSize, 
                maxConcurrentTasks, 
                maxTasksPerNode, 
                nodeFailures, 
                rackIds, 
                nodeIds, 
                newHashSet(dataBlockIds), 
                nodeToRackFunction, 
                dataBlockToNodesFunction, 
                nodeToDataBlocksFunction,
                taskNodeAllocator, 
                mapReduceJob);
    }
}
