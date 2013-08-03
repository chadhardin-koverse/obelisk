package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.event.Status;
import cehardin.nsu.mr.prioritize.replicate.event.StatusWriter;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static java.util.Collections.shuffle;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.replicateTaskSameSource;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.ClusterBuilder;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import cehardin.nsu.mr.prioritize.replicate.task.MapReduceTask;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import cehardin.nsu.mr.prioritize.replicate.task.Task;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import java.util.HashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class Simulator implements Callable<Double> {

    private final Logger logger = Logger.getLogger("Simulator");
    private final Variables variables;
    private final Cluster cluster;
    private final Supplier<StatusWriter> statusWriterSupplier;
    public Simulator(
            final Variables variables,
            final Supplier<StatusWriter> statusWriterSupplier) {
        final ClusterBuilder clusterBuilder = new ClusterBuilder();
        this.variables = variables;
        this.cluster = clusterBuilder.buildCluster(variables);
        this.statusWriterSupplier = statusWriterSupplier;
    }

    public Double call() throws Exception {
        final StatusWriter statusWriter = statusWriterSupplier.get();
        final List<Resource> resources = newArrayList();
        final List<Task> tasks = newLinkedList();
        final List<Task> runningTasks = newCopyOnWriteArrayList();
        final double totalTime = 1000000;
        final double timeStep = 1000;
        final AtomicInteger numMRTasksRunning = new AtomicInteger(0);
        final AtomicInteger numReplicateTasksRunning = new AtomicInteger(0);
        final AtomicInteger numMRTasksCompleted = new AtomicInteger(0);
        final AtomicInteger numReplicateTasksCompleted = new AtomicInteger(0);
//        final AtomicBoolean mapReduceJobStarted = new AtomicBoolean(false);
        double currentTime = 0;
        int numFailedNodes = 0;
        logger.info("Starting");
        logger.info("Cluster: " + cluster);
        resources.add(cluster.getNetworkResource());

        for (final Rack rack : cluster.getRacks()) {
            resources.add(rack.getNetworkResource());
            for (final Node node : rack.getNodes()) {
                resources.add(node.getDiskResource());
            }
        }

        while (currentTime < totalTime) {
            final double availableStepTime = (currentTime + timeStep) > totalTime ? totalTime - currentTime : timeStep;
            final Variables.MapReduceJob mapReduceJob = variables.getMapReduceJob();
            final TaskNodeAllocator allocator = variables.getTaskNodeAllocator();
            final Map<TaskId, NodeId> taskToNode;
            final Status status = new Status();
            int numSkipped = 0;
            
            System.out.printf("START.  Time=%s, EndTime=%s%n", currentTime, totalTime);
                
//            mapReduceJobStarted.set(true);

            taskToNode = allocator.allocate(
                    mapReduceJob.getTaskIds(),
                    cluster.getNodesById().keySet(),
                    Functions.forMap(cluster.getDataBlockIdToNodeIds(), new HashSet<NodeId>()),
                    mapReduceJob.getTaskIdToDataBlockId());

            System.out.printf("MR Scheduler scheduled %s tasks%n", taskToNode.size());

            for (final Map.Entry<TaskId, NodeId> entry : taskToNode.entrySet()) {
                final TaskId taskId = entry.getKey();
                final NodeId nodeId = entry.getValue();
                final DataBlockId dataBlockId = mapReduceJob.getTaskIdToDataBlockId().apply(taskId);
                
                if(cluster.getNodesById().containsKey(nodeId)) {
                    if(cluster.getNodesById().get(nodeId).getDataBlockIds().contains(dataBlockId)) {
                        final MapReduceTask mapReduceTask = new MapReduceTask(
                            cluster.getNodesById().get(nodeId),
                            cluster.getNodesById().get(nodeId).getDataBlockById().get(dataBlockId));

                        mapReduceJob.getTaskIds().remove(taskId);
                        tasks.add(mapReduceTask);
                    }
                    else {
                        numSkipped++;
                    }
                }
                else {
                    numSkipped++;
                }
            }
            
            if(!variables.getNodeFailures().isEmpty()) {
                final Iterator<Variables.NodeFailure> nodeFailures = variables.getNodeFailures().iterator();
                
                while(nodeFailures.hasNext()) {
                    final Variables.NodeFailure nodeFailure = nodeFailures.next();
                    
                    if(currentTime >= nodeFailure.getTimeUnit().toMillis(nodeFailure.getTime())) {
                        final NodeId nodeId = nodeFailure.getNodeId();
                        
                        for(final Rack rack : cluster.getRacks()) {
                            final Iterator<Node> nodes = rack.getNodes().iterator();
                            
                            while(nodes.hasNext()) {
                                final Node node = nodes.next();
                                
                                if(node.getId().equals(nodeId)) {
//                                    System.out.printf("Node has failed: %s%n", node.getId());
                                    nodes.remove();
                                    numFailedNodes++;
                                }
                            }
                        }
                    }
                }
            }

            /*if(all(tasks, not(instanceOf(ReplicateTask.class))))*/ {
                int count = 0;
                for(final ReplicateTask replicateTask : variables.getReplicateTaskScheduler().schedule(cluster)) {
                    if(all(runningTasks, not(replicateTaskSameSource(replicateTask)))) {
                        tasks.add(replicateTask);
                        count++;
                    }
                }
                
                if(count > 0) {
                    System.out.printf("Scheduled %s replicate tasks%n", count);
                }
            }

            shuffle(tasks);

            while (
                    !tasks.isEmpty() && 
                    (numMRTasksRunning.get() + numReplicateTasksRunning.get()) < variables.getMaxConcurrentTasks()) {
                final Task task = tasks.remove(0);
//                logger.log(Level.INFO, "Starting Task: {0}", task);
                if(MapReduceTask.class.isInstance(task)) {
                    numMRTasksRunning.incrementAndGet() ;
                } else {
                    numReplicateTasksRunning.incrementAndGet();
                }
                runningTasks.add(task);
                
                task.run(new Runnable() {
                    public void run() {
//                        logger.log(Level.INFO, "Task Finished: {0}", task);
                        if(MapReduceTask.class.isInstance(task)) {
                            numMRTasksRunning.decrementAndGet();
                            numMRTasksCompleted.incrementAndGet();
                        } else {
                            numReplicateTasksRunning.decrementAndGet();
                            numReplicateTasksCompleted.incrementAndGet();
                        }
                        runningTasks.remove(task);
                    }
                });
            }

            long time = 0;
            for (final Resource resource : resources) {
                time += resource.execute(availableStepTime);
            }
            
            currentTime += time / resources.size();
            
            System.out.printf("MR Job Tasks left: %s%n", variables.getMapReduceJob().getTaskIds().size());
            System.out.printf("# Data blocks with 3 copies: %s%n", cluster.getReplicationCounts().containsKey(3) ? cluster.getReplicationCounts().get(3).size() : 0);
            System.out.printf("# Data blocks with 2 copies: %s%n", cluster.getReplicationCounts().containsKey(2) ? cluster.getReplicationCounts().get(2).size() : 0);
            System.out.printf("# Data blocks with 1 copy: %s%n", cluster.getReplicationCounts().containsKey(1) ? cluster.getReplicationCounts().get(1).size() : 0);
            System.out.printf("# Data blocks with 0 copies: %s%n", difference(variables.getDataBlockIds(),cluster.getDataBlockIdToNodeIds().keySet()).size());
            System.out.printf("# Tasks: %s%n", tasks.size());
            System.out.printf(
                    "STOP: Current Time: %s.  Tasks Running: %s / %s.  Tasks Completed: %s / %s.  Failed Nodes: %s.  Tasks skipped: %s%n", 
                    currentTime, 
                    numMRTasksRunning.get(), 
                    numReplicateTasksRunning.get(),
                    numMRTasksCompleted.get(),
                    numReplicateTasksCompleted.get(),
                    numFailedNodes,
                    numSkipped);
            System.out.println();
            
            
            status.setTime((long)currentTime);
            status.setNumNodes(variables.getNodeIds().size());
            status.setNumFailedNodes(variables.getNodeIds().size() - cluster.getNodesById().size());
            status.setNumDataBlocks(variables.getDataBlockIds().size());
            status.setNumMRTasks(numMRTasksCompleted.get());
            status.setNumReplicaTasks(numReplicateTasksCompleted.get());
            status.setReplicaCount(new HashMap<DataBlockId,Integer>(cluster.getDataBlockCount()));
            for(DataBlockId dataBlockId : difference(variables.getDataBlockIds(), cluster.getDataBlockCount().keySet())) {
                status.getReplicaCount().put(dataBlockId, 0);
            }
            
            statusWriter.write(status);
            
            
            if(
                    tasks.isEmpty() && 
                    numMRTasksRunning.get() == 0 && 
                    numReplicateTasksRunning.get() == 0 ) { //&&
//                    (mapReduceJob.getTaskIds().isEmpty() || mapReduceJob.getTaskIds().size() == numSkipped)) {
                System.out.printf("END%n%n");
                break;
            }
        }

        System.out.printf("REPLICATION COUNTS%n");
        System.out.printf("COUNT\tNUM BLOCKS%n");
        for(final Map.Entry<Integer, Set<DataBlockId>> entry : cluster.getReplicationCounts().entrySet()) {
            final int count = entry.getKey();
            final Set<DataBlockId> dataBlocks = entry.getValue();
            final int numBlocks = dataBlocks.size();
            System.out.printf("%s\t\t%s%n", count, numBlocks);
        }
        
        System.out.printf("%s\t\t%s%n", 0, difference(variables.getDataBlockIds(),cluster.getDataBlocksById().keySet()).size());
        
        statusWriter.close();
        
        return currentTime;
    }
    
    public Cluster getCluster() {
        return cluster;
    }
}
