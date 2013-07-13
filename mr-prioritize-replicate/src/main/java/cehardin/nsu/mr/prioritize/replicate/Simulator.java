package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.shuffle;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class Simulator implements Callable<Object> {

    private final Logger logger = Logger.getLogger("Simulator");
    private final ExecutorService executorService;
    private final Variables variables;
    private final Cluster cluster;

    public Simulator(
            final Variables variables,
            final ExecutorService executorService) {
        final ClusterBuilder clusterBuilder = new ClusterBuilder();
        this.variables = variables;
        this.executorService = executorService;
        this.cluster = clusterBuilder.buildCluster(variables);
    }

    public Object call() throws Exception {
        final List<Resource> resources = newArrayList();
        final List<Task> tasks = newLinkedList();
        final double totalTime = 1000000;
        final double timeStep = 1000;
        final AtomicInteger numRunningTasks = new AtomicInteger(0);
        final AtomicBoolean mapReduceJobStarted = new AtomicBoolean(false);
        double currentTime = 0;

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
            final Collection<Future<Double>> futures = new ArrayList<Future<Double>>(resources.size());

            if (!mapReduceJobStarted.get()) {
                final Variables.MapReduceJob mapReduceJob = variables.getMapReduceJob();
                final long mapReduceJobStartTime = mapReduceJob.getTimeUnit().toMillis(mapReduceJob.getStartTime());

                if (mapReduceJobStartTime >= currentTime) {
                    final TaskNodeAllocator allocator = variables.getTaskNodeAllocator();
                    final Map<TaskId, NodeId> taskToNode;

                    mapReduceJobStarted.set(true);

                    taskToNode = allocator.allocate(
                            mapReduceJob.getTaskIds(),
                            variables.getNodeIds(),
                            variables.getDataBlockIdToNodeIds(),
                            mapReduceJob.getTaskIdToDataBlockId());

                    for (final Map.Entry<TaskId, NodeId> entry : taskToNode.entrySet()) {
                        final TaskId taskId = entry.getKey();
                        final NodeId nodeId = entry.getValue();
                        final DataBlockId dataBlockId = mapReduceJob.getTaskIdToDataBlockId().apply(taskId);
                        final MapReduceTask mapReduceTask = new MapReduceTask(
                                cluster.getNodesById().get(nodeId),
                                cluster.getNodesById().get(nodeId).getDataBlockById().get(dataBlockId));

                        tasks.add(mapReduceTask);
                    }
                }
            }

            if (!contains(tasks, instanceOf(ReplicateTask.class))) {
                final ReplicateTaskScheduler replicateTaskScheduler = variables.getReplicateTaskScheduler();
                tasks.addAll(replicateTaskScheduler.schedule(cluster));
            }

            shuffle(tasks);

            if (!tasks.isEmpty() && numRunningTasks.get() < variables.getMaxConcurrentTasks()) {
                final Task task = tasks.remove(0);
                logger.log(Level.INFO, "Starting Task: {0}", task);
                task.run(new Runnable() {
                    public void run() {
                        logger.log(Level.INFO, "Task Finished: {0}", task);
                        numRunningTasks.decrementAndGet();
                    }
                });
            }

            for (final Resource resource : resources) {
                futures.add(executorService.submit(new Callable<Double>() {
                    public Double call() throws Exception {
                        return resource.execute(availableStepTime);
                    }
                }));
            }

            for (final Future<Double> future : futures) {
                currentTime += future.get();
            }
            
            logger.log(Level.INFO, "Current Time: {0}", currentTime);
            
            if(tasks.isEmpty()) {
                break;
            }
        }

        return currentTime;
    }
}
