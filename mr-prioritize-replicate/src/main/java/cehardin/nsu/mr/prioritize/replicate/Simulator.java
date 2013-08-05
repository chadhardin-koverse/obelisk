package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.event.Status;
import cehardin.nsu.mr.prioritize.replicate.event.StatusWriter;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.removeIf;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.replicateTaskSameBlock;

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
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
    private final ReplicateTaskScheduler replicateTaskScheduler;

    public Simulator(
            final Variables variables,
            final Supplier<StatusWriter> statusWriterSupplier,
            final ReplicateTaskScheduler replicateTaskScheduler) {
        final ClusterBuilder clusterBuilder = new ClusterBuilder();
        this.variables = variables;
        this.cluster = clusterBuilder.buildCluster(variables);
        this.statusWriterSupplier = statusWriterSupplier;
        this.replicateTaskScheduler = replicateTaskScheduler;
    }

    @Override
    public Double call() throws Exception {
        final StatusWriter statusWriter = statusWriterSupplier.get();
        final List<Resource> resources = newArrayList();
//        final double totalTime = 1000000;
        final double mapReduceTimeStep = TimeUnit.SECONDS.toMillis(4);
        final double replicaTimeStep = TimeUnit.MINUTES.toMillis(5);
        final List<MapReduceTask> runningMapReduceTasks = newCopyOnWriteArrayList();
        final List<ReplicateTask> runningReplicateTasks = newCopyOnWriteArrayList();
        final AtomicInteger numMRTasksCompleted = new AtomicInteger(0);
        final AtomicInteger numReplicateTasksCompleted = new AtomicInteger(0);
        final AtomicInteger numMRTasksKilled = new AtomicInteger(0);
        final AtomicInteger numReplicateTasksKilled = new AtomicInteger(0);
        double currentTime = 0;
        int numFailedNodes = 0;

        logger.info("Starting");

        resources.add(cluster.getNetworkResource());
        for (final Rack rack : cluster.getRacks()) {
            resources.add(rack.getNetworkResource());
            for (final Node node : rack.getNodes()) {
                resources.add(node.getDiskResource());
            }
        }

        while (true) {
//            final double availableStepTime = (currentTime + timeStep) > totalTime ? totalTime - currentTime : timeStep;
            final Variables.MapReduceJob mapReduceJob = variables.getMapReduceJob();
            final TaskNodeAllocator allocator = variables.getTaskNodeAllocator();
            final Map<TaskId, NodeId> taskToNode;
            final Status status = new Status();
            final List<MapReduceTask> mapReduceTasks = newArrayList();
            final List<ReplicateTask> replicateTasks = newArrayList();
            
            System.out.printf("START.  Time=%s%n", currentTime);

            taskToNode = allocator.allocate(
                    mapReduceJob.getTaskIds(),
                    cluster.getNodesById().keySet(),
                    Functions.forMap(cluster.getDataBlockIdToNodeIds(), new HashSet<NodeId>()),
                    mapReduceJob.getTaskIdToDataBlockId());

            for (final Map.Entry<TaskId, NodeId> entry : taskToNode.entrySet()) {
                final TaskId taskId = entry.getKey();
                final NodeId nodeId = entry.getValue();
                final DataBlockId dataBlockId = mapReduceJob.getTaskIdToDataBlockId().apply(taskId);

                if ((runningMapReduceTasks.size() + mapReduceTasks.size()) < variables.getMaxConcurrentTasks()) {
                    if (cluster.getNodesById().containsKey(nodeId)) {
                        if (cluster.getNodesById().get(nodeId).hasDataBlock(dataBlockId)) {
                            final MapReduceTask mapReduceTask = new MapReduceTask(
                                    taskId,
                                    cluster.getNodesById().get(nodeId),
                                    cluster.getNodesById().get(nodeId).getDataBlockById().get(dataBlockId));

                            mapReduceJob.getTaskIds().remove(taskId);
                            mapReduceTasks.add(mapReduceTask);
                        }
                    }
                }
            }

            System.out.printf("Scheduled %s MR tasks%n", mapReduceTasks.size());

            if (!variables.getNodeFailures().isEmpty()) {
                final Iterator<Variables.NodeFailure> nodeFailures = variables.getNodeFailures().iterator();
                
                while (nodeFailures.hasNext()) {
                    final Variables.NodeFailure nodeFailure = nodeFailures.next();

                    if (currentTime >= nodeFailure.getTimeUnit().toMillis(nodeFailure.getTime())) {
                        final NodeId nodeId = nodeFailure.getNodeId();

                        for (final Rack rack : cluster.getRacks()) {
                            final Iterator<Node> nodes = rack.getNodes().iterator();

                            while (nodes.hasNext()) {
                                final Node node = nodes.next();

                                if (node.getId().equals(nodeId)) {
//                                    System.out.printf("Node has failed: %s%n", node.getId());
                                    nodes.remove();
                                    numFailedNodes++;
                                    
                                    resources.remove(node.getDiskResource());
                                    
                                    for(final MapReduceTask mapReduceTask : filter(concat(runningMapReduceTasks, mapReduceTasks), MapReduceTask.mapReduceTaskReliesOnNode(node))) {
                                        final TaskId taskId = mapReduceTask.getTaskId();
                                        variables.getMapReduceJob().getTaskIds().add(taskId);
                                    }
                                    
                                    for(final MapReduceTask mapReduceTask : filter(runningMapReduceTasks, MapReduceTask.mapReduceTaskReliesOnNode(node))) {
                                        numMRTasksKilled.incrementAndGet();
                                    }
                                    
                                    for(final ReplicateTask replicateTask : filter(runningReplicateTasks, ReplicateTask.replicateTaskReliesOnNode(node))) {
                                        numReplicateTasksKilled.incrementAndGet();
                                    }
                                    
                                    removeIf(runningMapReduceTasks, MapReduceTask.mapReduceTaskReliesOnNode(node));
                                    removeIf(mapReduceTasks, MapReduceTask.mapReduceTaskReliesOnNode(node));
                                    removeIf(runningReplicateTasks, ReplicateTask.replicateTaskReliesOnNode(node));
                                }
                            }
                        }
                    }
                }
            }

            for (final ReplicateTask replicateTask : replicateTaskScheduler.schedule(cluster, runningReplicateTasks)) {
                replicateTasks.add(replicateTask);
            }

            System.out.printf("Scheduled %s replicate tasks%n", replicateTasks.size());


            for (final ReplicateTask replicateTask : replicateTasks) {
                runningReplicateTasks.add(replicateTask);
                replicateTask.run(new Runnable() {
                    @Override
                    public void run() {
                        numReplicateTasksCompleted.incrementAndGet();
                        runningReplicateTasks.remove(replicateTask);
                    }
                });
            }

            for (final MapReduceTask mapReduceTask : mapReduceTasks) {
                runningMapReduceTasks.add(mapReduceTask);
                mapReduceTask.run(new Runnable() {
                    @Override
                    public void run() {
                        numMRTasksCompleted.incrementAndGet();
                        runningMapReduceTasks.remove(mapReduceTask);
                    }
                });
            }

            
            {
                final double availableStepTime = runningMapReduceTasks.isEmpty() ? replicaTimeStep : mapReduceTimeStep;
                double time = 0;
                
                for (final Resource resource : resources) {
                    time = Math.max(time, resource.execute(availableStepTime));
                }

                currentTime += time;
            }

            System.out.printf("MR Job Tasks left: %s%n", variables.getMapReduceJob().getTaskIds().size());
            System.out.printf("# Data blocks with 3 copies: %s%n", cluster.getReplicationCounts().containsKey(3) ? cluster.getReplicationCounts().get(3).size() : 0);
            System.out.printf("# Data blocks with 2 copies: %s%n", cluster.getReplicationCounts().containsKey(2) ? cluster.getReplicationCounts().get(2).size() : 0);
            System.out.printf("# Data blocks with 1 copy: %s%n", cluster.getReplicationCounts().containsKey(1) ? cluster.getReplicationCounts().get(1).size() : 0);
            System.out.printf("# Data blocks with 0 copies: %s%n", difference(variables.getDataBlockIds(), cluster.getDataBlockIdToNodeIds().keySet()).size());

            System.out.printf(
                    "STOP: Current Time: %s.  Tasks Running: %s / %s.  Tasks Completed: %s / %s.  Tasks Killed: %s / %s.  Failed Nodes: %s%n",
                    currentTime,
                    runningMapReduceTasks.size(),
                    runningReplicateTasks.size(),
                    numMRTasksCompleted.get(),
                    numReplicateTasksCompleted.get(),
                    numMRTasksKilled.get(),
                    numReplicateTasksKilled.get(),
                    numFailedNodes);
            System.out.println();


            status.setTime((long) currentTime);
            status.setNumNodes(variables.getNodeIds().size());
            status.setNumFailedNodes(variables.getNodeIds().size() - cluster.getNodesById().size());
            status.setNumDataBlocks(variables.getDataBlockIds().size());
            status.setNumMRTasks(numMRTasksCompleted.get());
            status.setNumReplicaTasks(numReplicateTasksCompleted.get());
            status.setReplicaCount(new HashMap<>(cluster.getDataBlockCount()));
            status.setNumMRTasksLeft(variables.getMapReduceJob().getTaskIds().size());
            status.setNumMRTasksKilled(numMRTasksKilled.get());
            status.setNumReplicateTasksKilled(numReplicateTasksKilled.get());
            
            for (DataBlockId dataBlockId : difference(variables.getDataBlockIds(), cluster.getDataBlockCount().keySet())) {
                status.getReplicaCount().put(dataBlockId, 0);
            }

            statusWriter.write(status);


            if (variables.getMapReduceJob().getTaskIds().isEmpty() 
                    && runningReplicateTasks.isEmpty()
                    && runningMapReduceTasks.isEmpty()) {
                System.out.printf("END%n%n");
                break;
            }
        }

        System.out.printf("REPLICATION COUNTS%n");
        System.out.printf("COUNT\tNUM BLOCKS%n");
        for (final Map.Entry<Integer, Set<DataBlockId>> entry : cluster.getReplicationCounts().entrySet()) {
            final int count = entry.getKey();
            final Set<DataBlockId> dataBlocks = entry.getValue();
            final int numBlocks = dataBlocks.size();
            System.out.printf("%s\t\t%s%n", count, numBlocks);
        }

        System.out.printf("%s\t\t%s%n", 0, difference(variables.getDataBlockIds(), cluster.getDataBlocksById().keySet()).size());

        statusWriter.close();

        return currentTime;
    }

    public Cluster getCluster() {
        return cluster;
    }
}
