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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cehar_000
 */
public class App implements Runnable {
    private final Variables variables;
    
    public App(Variables variables) {
        this.variables = variables;
    }

    @Override
    public void run() {
        try {
            final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            final Simulator simulator = new Simulator(variables, executorService);
            final double time = simulator.call();
            System.out.printf("Elapsed time: %s", time);
            executorService.shutdown();
        }   
        catch(Exception e) {
            throw new RuntimeException("Failed", e);
        }
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
        final int numRacks;
        final int numDataBlocks;
        final int numTasks;
        final Variables variables;
        
        diskBadwidth = new Bandwidth(100_000_000L, 1, TimeUnit.SECONDS);
        rackBandwidth = new Bandwidth(1_000_000_000L / 8, 1, TimeUnit.SECONDS);
        clusterBandwidth = new Bandwidth(10_000_000_000L / 8, 1, TimeUnit.SECONDS);
        blockSize = 128 * 1024 * 1024;
        numNodes = 1024;
        numRacks = numNodes / 16;
        numDataBlocks = numNodes * 2;
        maxConcurrentTasks = numNodes * 4;
        maxTasksPerNode = 2;
        nodePercentageFailed = 0.05;
        numTasks = numNodes * 10;
        
        VariablesFactory variablesFactory = new VariablesFactory(
                random, 
                diskBadwidth, 
                rackBandwidth, 
                clusterBandwidth, 
                blockSize, 
                numNodes, 
                numRacks, 
                numDataBlocks, 
                maxConcurrentTasks, 
                maxTasksPerNode, 
                nodePercentageFailed, 
                numTasks);
        variables = variablesFactory.get();
        
        App app = new App(variables);
        app.run();
        
    }
}

