package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.event.Status;
import cehardin.nsu.mr.prioritize.replicate.event.StatusWriter;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.removeIf;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.isReplicateTaskCritical;
import static cehardin.nsu.mr.prioritize.replicate.hardware.Node.extractIdFromHardware;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.ClusterBuilder;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import cehardin.nsu.mr.prioritize.replicate.task.MapReduceTask;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractNodesFromReplicateTask;
import com.google.common.base.Supplier;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import java.util.HashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
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
    final MapReduceTaskScheduler mapReduceTaskScheduler;

    public Simulator(
            final Variables variables,
            final Supplier<StatusWriter> statusWriterSupplier,
            final ReplicateTaskScheduler replicateTaskScheduler,
            final MapReduceTaskScheduler mapReduceTaskScheduler) {
        final ClusterBuilder clusterBuilder = new ClusterBuilder();
        this.variables = variables;
        this.cluster = clusterBuilder.buildCluster(variables);
        this.statusWriterSupplier = statusWriterSupplier;
        this.replicateTaskScheduler = replicateTaskScheduler;
        this.mapReduceTaskScheduler = mapReduceTaskScheduler;
    }

    @Override
    public Double call() throws Exception {
        final StatusWriter statusWriter = statusWriterSupplier.get();
        final List<Resource> resources = newArrayList();
//        final double totalTime = 1000000;
        final double mapReduceTimeStep = TimeUnit.SECONDS.toMillis(10);
        final double replicaTimeStep = TimeUnit.MINUTES.toMillis(60);
        final List<MapReduceTask> runningMapReduceTasks = newCopyOnWriteArrayList();
        final List<ReplicateTask> runningReplicateTasks = newCopyOnWriteArrayList();
        final AtomicInteger numMRTasksCompleted = new AtomicInteger(0);
        final AtomicInteger numCriticalReplicateTasksCompleted = new AtomicInteger(0);
        final AtomicInteger numNonCriticalReplicateTasksCompleted = new AtomicInteger(0);
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
            final Variables.MapReduceJob mapReduceJob = variables.getMapReduceJob();
//            final TaskNodeAllocator allocator = variables.getTaskNodeAllocator();
            final Map<TaskId, NodeId> taskToNode;
            final Status status = new Status();
            final List<MapReduceTask> mapReduceTasks = newArrayList();
            final List<ReplicateTask> replicateTasks = newArrayList();
            final double timeElapsed;
            
            System.out.printf("START.  Time=%,d%n", (long)currentTime);


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
            System.out.printf("Scheduled %,d (%,d / %,d) replicate tasks%n", replicateTasks.size(), size(filter(replicateTasks, isReplicateTaskCritical())), size(filter(replicateTasks, not(isReplicateTaskCritical()))));
            
            if(currentTime >= mapReduceJob.getStartTime()) {
                for(final MapReduceTask mapReduceTask : mapReduceTaskScheduler.schedule(cluster, mapReduceJob, newHashSet(transform(concat(transform(filter(concat(runningReplicateTasks, replicateTasks), isReplicateTaskCritical()), extractNodesFromReplicateTask())), extractIdFromHardware(NodeId.class))), variables.getMaxConcurrentTasks() - runningMapReduceTasks.size())) {
                    mapReduceJob.getTaskIds().remove(mapReduceTask.getTaskId());
                    mapReduceTasks.add(mapReduceTask);
                }
                System.out.printf("Scheduled %,d MR tasks%n", mapReduceTasks.size());
            }
            else {
                System.out.printf("Not starting MR job until time %,d%n", mapReduceJob.getStartTime());
            }


            for (final ReplicateTask replicateTask : replicateTasks) {
                runningReplicateTasks.add(replicateTask);
                replicateTask.run(new Runnable() {
                    @Override
                    public void run() {
                        if(isReplicateTaskCritical().apply(replicateTask)) {
                            numCriticalReplicateTasksCompleted.incrementAndGet();
                        }
                        else {
                            numNonCriticalReplicateTasksCompleted.incrementAndGet();
                        }
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
                final double availableStepTime;
                double time = 0;
                
                if(mapReduceJob.getTaskIds().isEmpty() && runningMapReduceTasks.isEmpty()) {
                    availableStepTime = replicaTimeStep;
                }
                else {
                    availableStepTime = mapReduceTimeStep;
                }
                
                for (final Resource resource : resources) {
                    time = Math.max(time, resource.execute(availableStepTime));
                }

                timeElapsed = time;
                currentTime += timeElapsed;
            }

            System.out.printf("MR Job Tasks left: %,d%n", variables.getMapReduceJob().getTaskIds().size());
            System.out.printf("# Data blocks with 3 copies: %,d%n", cluster.getReplicationCounts().containsKey(3) ? cluster.getReplicationCounts().get(3).size() : 0);
            System.out.printf("# Data blocks with 2 copies: %,d%n", cluster.getReplicationCounts().containsKey(2) ? cluster.getReplicationCounts().get(2).size() : 0);
            System.out.printf("# Data blocks with 1 copy: %,d%n", cluster.getReplicationCounts().containsKey(1) ? cluster.getReplicationCounts().get(1).size() : 0);
            System.out.printf("# Data blocks with 0 copies: %,d%n", difference(variables.getDataBlockIds(), cluster.getDataBlockIdToNodeIds(Executors.newSingleThreadExecutor()).keySet()).size());

            System.out.printf(
                    "STOP: Current Time: %,d (%,d).  Tasks Running: %,d / %,d (%,d / %,d).  Tasks Completed: %,d / %,d (%,d / %,d).  Tasks Killed: %,d / %,d.  Failed Nodes: %,d%n",
                    (long)currentTime,
                    (long)timeElapsed,
                    runningMapReduceTasks.size(),
                    runningReplicateTasks.size(),
                    size(filter(runningReplicateTasks, isReplicateTaskCritical())),
                    size(filter(runningReplicateTasks, not(isReplicateTaskCritical()))),
                    numMRTasksCompleted.get(),
                    numCriticalReplicateTasksCompleted.get() + numNonCriticalReplicateTasksCompleted.get(),
                    numCriticalReplicateTasksCompleted.get(),
                    numNonCriticalReplicateTasksCompleted.get(),
                    numMRTasksKilled.get(),
                    numReplicateTasksKilled.get(),
                    numFailedNodes);
            System.out.println();


            status.setTime((long) currentTime);
            status.setTimeStep((long)timeElapsed);
            status.setNumNodes(variables.getNodeIds().size());
            status.setNumFailedNodes(variables.getNodeIds().size() - cluster.getNodesById().size());
            status.setNumDataBlocks(variables.getDataBlockIds().size());
            status.setNumMRTasks(numMRTasksCompleted.get());
            status.setNumReplicaTasks(numCriticalReplicateTasksCompleted.get() + numNonCriticalReplicateTasksCompleted.get());
            status.setNumCriticalReplicaTasks(numCriticalReplicateTasksCompleted.get());
            status.setNumNonCriticalReplicTasks(numNonCriticalReplicateTasksCompleted.get());
            status.setReplicaCount(new HashMap<>(cluster.getDataBlockCount()));
            status.setNumMRTasksLeft(variables.getMapReduceJob().getTaskIds().size());
            status.setNumMRTasksKilled(numMRTasksKilled.get());
            status.setNumReplicateTasksKilled(numReplicateTasksKilled.get());
            
            for (DataBlockId dataBlockId : difference(variables.getDataBlockIds(), cluster.getDataBlockCount().keySet())) {
                status.getReplicaCount().put(dataBlockId, 0);
            }

            statusWriter.write(status);


            if ( !cluster.getReplicationCounts().containsKey(1) &&
                    !cluster.getReplicationCounts().containsKey(2) &&
                    runningReplicateTasks.isEmpty() &&
                    runningMapReduceTasks.isEmpty()) {
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
            System.out.printf("%,d\t\t%,d%n", count, numBlocks);
        }

        System.out.printf("%,d\t\t%,d%n", 0, difference(variables.getDataBlockIds(), cluster.getDataBlocksById().keySet()).size());

        statusWriter.close();

        return currentTime;
    }

    public Cluster getCluster() {
        return cluster;
    }
}
