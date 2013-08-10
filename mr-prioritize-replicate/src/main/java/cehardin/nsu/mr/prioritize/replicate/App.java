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
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import java.io.File;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
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
            final HotDataBlockReplicateTaskScheduler hotDataBlockReplicateTaskScheduler = new HotDataBlockReplicateTaskScheduler(random, executorService, createTemperatureMap(variables.getNodeFailures(), variables.getNodeIds(), variables.getNodeIdToDataBlockIds()));
            
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
    
    private Map<DataBlockId, Double> createTemperatureMap(Iterable<Variables.NodeFailure> nodeFailures, Iterable<NodeId> nodes, Function<NodeId, Set<DataBlockId>> nodeToDataBlocks) {
        final Map<DataBlockId, Double> temperatureMap = new HashMap<>();
        
        for(final NodeId nodeId : nodes) {
            for(final DataBlockId dataBlockId : nodeToDataBlocks.apply(nodeId)) {
                temperatureMap.put(dataBlockId, 0.0);
            }
        }
        
        for(final Variables.NodeFailure nodeFailure : nodeFailures) {
            final long time = nodeFailure.getTimeUnit().toMillis(nodeFailure.getTime());
            final double temperature = Long.MAX_VALUE - time;
            
            for(final DataBlockId dataBlockId : nodeToDataBlocks.apply(nodeFailure.getNodeId())) {
                final double currentTemperature = temperatureMap.get(dataBlockId);
                temperatureMap.put(dataBlockId, Math.max(temperature, currentTemperature));
            }
        }
        
        return temperatureMap;
        
        
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
        final long mapReduceStartTime;
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
        numDataBlocks = numNodes * 100;
        maxConcurrentTasks = numNodes * 4;
        maxTasksPerNode = 1;
        nodePercentageFailed = 0.10;
        mapReduceStartTime = TimeUnit.SECONDS.toMillis(20);
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
                mapReduceStartTime,
                numTasks);
        
        System.out.printf("Creating variables...%n");
        variables = variablesFactory.get();
        System.out.printf("Created variables%n");
        App app = new App(variables, random);
        app.run();
        
    }
}

