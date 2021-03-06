/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;


import cehardin.nsu.mr.prioritize.replicate.Variables.Bandwidth;
import cehardin.nsu.mr.prioritize.replicate.event.CsvStatusWriterSupplier;
import cehardin.nsu.mr.prioritize.replicate.event.DateSequenceFileSupplier;
import cehardin.nsu.mr.prioritize.replicate.event.FileWriterSupplier;
import cehardin.nsu.mr.prioritize.replicate.event.StatusWriter;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import java.io.File;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author cehar_000
 */
public class App implements Runnable {
    private final Variables variables;
    private final Random random;
    
    public App(Variables variables, Random random) {
        this.variables = variables;
        this.random = random;
    }

    @Override
    public void run() {
        try {
            final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            final File outputDir = new File(System.getProperty("user.home"), "mapreduce-simulation");
            outputDir.mkdirs();
            final Supplier<File> fileSupplier = new DateSequenceFileSupplier(outputDir, "mapreduce-simulation", "csv", new Date(), 1);
            final Supplier<Writer> writerSupplier = new FileWriterSupplier(fileSupplier);
            final Supplier<StatusWriter> statusWriterSupplier = new CsvStatusWriterSupplier(writerSupplier);
            final MapReduceTaskScheduler mapReduceTaskScheduler = new StandardMapReduceTaskScheduler(executorService);
            final StandardReplicateTaskScheduler standardReplicateTaskScheduler = new StandardReplicateTaskScheduler(random, executorService);
            final HotDataBlockReplicateTaskScheduler hotDataBlockReplicateTaskScheduler = new HotDataBlockReplicateTaskScheduler(random, executorService, createTemperatureMap(variables));
            
            run(statusWriterSupplier, standardReplicateTaskScheduler, mapReduceTaskScheduler);
            run(statusWriterSupplier, hotDataBlockReplicateTaskScheduler, mapReduceTaskScheduler);
            
            System.out.printf("Output is at %s%n", outputDir.getCanonicalPath());
            executorService.shutdown();
        }   
        catch(Exception e) {
            throw new RuntimeException("Failed", e);
        }
    }
    
    private void run(Supplier<StatusWriter> statusWriterSupplier, ReplicateTaskScheduler replicateTaskScheduler, MapReduceTaskScheduler mapReduceTaskScheduler) {
        try {
            final Simulator simulator = new Simulator(variables.clone(), statusWriterSupplier, replicateTaskScheduler, mapReduceTaskScheduler);
            final double time = simulator.call();
            System.out.printf("Elapsed time for supplier %s: %,d%n", replicateTaskScheduler.getClass(), (long)time);
        }   
        catch(Exception e) {
            throw new RuntimeException("Failed", e);
        }
    }
    
    private Map<DataBlockId, Double> createTemperatureMap(Variables variables) {
        final Variables.MapReduceJob mapReduceJob = variables.getMapReduceJob();
        final Map<DataBlockId, Double> temperatureMap = new HashMap<>();
        final Set<NodeId> hotNodes = new HashSet<>();
        
        for(final NodeId nodeId : variables.getNodeIds()) {
            for(final DataBlockId dataBlockId : variables.getNodeIdToDataBlockIds().apply(nodeId)) {
                temperatureMap.put(dataBlockId, 0.0);
            }
        }
        
        for(final TaskId taskId : mapReduceJob.getTaskIds()) {
            final DataBlockId dataBlockId = mapReduceJob.getTaskIdToDataBlockId().apply(taskId);
            temperatureMap.put(dataBlockId, temperatureMap.get(dataBlockId) + 1);
            hotNodes.addAll(variables.getDataBlockIdToNodeIds().apply(dataBlockId));
        }
        
        for(final NodeId hotNode : hotNodes) {
            for(final DataBlockId dataBlockId : variables.getNodeIdToDataBlockIds().apply(hotNode)) {
                temperatureMap.put(dataBlockId, temperatureMap.get(dataBlockId) + 1);
            }            
        }
        
        return temperatureMap;
        
        
    }
    
    public static void main(String[] args) {
        final Bandwidth diskBadwidth;
        final Bandwidth rackBandwidth;
        final Bandwidth clusterBandwidth;
        final int blockSize;
        final int numNodes;
        final int maxConcurrentTasks;
        final int maxTasksPerNode;
        final double nodePercentageFailed;
        final long mapReduceStartTime;
        final int numRacks;
        final int numDataBlocks;
        final int numTasks;
        
        
        diskBadwidth = new Bandwidth(100_000_000L, 1, TimeUnit.SECONDS);
        rackBandwidth = new Bandwidth(1_000_000_000L / 8, 1, TimeUnit.SECONDS);
        clusterBandwidth = new Bandwidth(10_000_000_000L / 8, 1, TimeUnit.SECONDS);
        blockSize = 128 * 1024 * 1024;
        numNodes = 1024;
        numRacks = numNodes / 16;
        numDataBlocks = numNodes * 100;
        maxConcurrentTasks = numNodes * 4;
        maxTasksPerNode = 1;
        nodePercentageFailed = 0.60;
        mapReduceStartTime = TimeUnit.SECONDS.toMillis(60);
        numTasks = numNodes / 2;
        
        
        
        
        
        
        for(int runNumber=1; runNumber <= 10; runNumber++) {
            final Random random = new Random(runNumber);
            final Variables variables;
            final VariablesFactory variablesFactory = new VariablesFactory(
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
                mapReduceStartTime,
                numTasks);
            final App app;
        
            System.out.printf("Run # %,d : Creating variables...%n", runNumber);
            variables = variablesFactory.get();
            System.out.printf("Run # %,d : Created variables%n", runNumber);
            
            app = new App(variables, random);
            app.run();
        }
        
        
    }
}

