/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Sets.newHashSet;

import cehardin.nsu.mr.prioritize.replicate.Variables.Bandwidth;
import cehardin.nsu.mr.prioritize.replicate.Variables.MapReduceJob;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.ClusterBuilder;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import com.google.common.base.Function;
import java.util.Set;
import java.util.SortedSet;
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
        final Bandwidth diskBadwidth;
        final Bandwidth rackBandwidth;
        final Bandwidth clusterBandwidth;
        final int blockSize;
        final int numNodes;
        final int maxConcurrentTasks;
        final int maxTasksPerNode;
        final SortedSet nodeFailures;
        final Set<RackId> rackIds;
        final Set<NodeId> nodeIds;
        final Set<DataBlockId> dataBlockIds;
        final Function<NodeId, RackId> nodeIdToRackId;
        final Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds;
        final TaskNodeAllocator taskNodeAllocator;
        final ReplicateTaskScheduler replicateTaskScheduler;
        final MapReduceJob mapReduceJob;
        final Variables variables;
        
        diskBadwidth = new Bandwidth(100_000_000L, 1, TimeUnit.SECONDS);
        rackBandwidth = new Bandwidth(1_000_000_000L / 8, 1, TimeUnit.SECONDS);
        clusterBandwidth = new Bandwidth(10_000_000_000L / 8, 1, TimeUnit.SECONDS);
        blockSize = 128 * 1024 * 1024;
        numNodes = 1024;
        maxConcurrentTasks = numNodes * 8;
        maxTasksPerNode = 2;
        
        variables = new Variables
    }
}
