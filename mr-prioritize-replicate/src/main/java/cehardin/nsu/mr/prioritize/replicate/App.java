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
import com.google.common.base.Supplier;
import java.io.File;
import java.io.Writer;
import java.util.Date;
import java.util.Random;
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
            final File outputDir = new File(System.getProperty("user.home"), "mapreduce-simulation");
            outputDir.mkdirs();
            final Supplier<File> fileSupplier = new DateSequenceFileSupplier(outputDir, "mapreduce-simulation", "csv", new Date(), 1);
            final Supplier<Writer> writerSupplier = new FileWriterSupplier(fileSupplier);
            final Supplier<StatusWriter> statusWriterSupplier = new CsvStatusWriterSupplier(writerSupplier);
            final Simulator simulator = new Simulator(variables, statusWriterSupplier);
            final double time = simulator.call();
            System.out.printf("Elapsed time: %s%n", time);
            System.out.printf("Output is at %s%n", outputDir.getCanonicalPath());
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
        final double nodePercentageFailed1;
        final double nodePercentageFailed2;
        final long nodeFail2Time;
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
        nodePercentageFailed1 = 0.10;
        nodePercentageFailed2 = 0.10;
        nodeFail2Time = TimeUnit.SECONDS.toMillis(10);
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
                nodePercentageFailed1, 
                nodePercentageFailed2,
                nodeFail2Time,
                numTasks);
        variables = variablesFactory.get();
        
        App app = new App(variables);
        app.run();
        
    }
}

