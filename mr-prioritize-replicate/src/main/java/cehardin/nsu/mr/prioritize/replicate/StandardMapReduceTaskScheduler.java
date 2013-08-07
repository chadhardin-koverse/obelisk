/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import cehardin.nsu.mr.prioritize.replicate.task.MapReduceTask;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Chad
 */
public class StandardMapReduceTaskScheduler implements MapReduceTaskScheduler {

    private final ExecutorService executorService;

    public StandardMapReduceTaskScheduler(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public List<MapReduceTask> schedule(final Cluster cluster, final Variables.MapReduceJob mapReduceJob, final Set<NodeId> workingNodes, final int maxTasks) {
        final List<MapReduceTask> tasks = new ArrayList<>();
        final List<Future<Optional<MapReduceTask>>> futures = new ArrayList<>();
        final AtomicInteger numTasks = new AtomicInteger(0);

        for (final TaskId taskId : mapReduceJob.getTaskIds()) {
            final DataBlockId dataBlockId = mapReduceJob.getTaskIdToDataBlockId().apply(taskId);
            futures.add(executorService.submit(new Callable<Optional<MapReduceTask>>() {
                @Override
                public Optional<MapReduceTask> call() throws Exception {
                    if (numTasks.incrementAndGet() <= maxTasks) {
                        for (final Rack rack : cluster.getRacks()) {
                            for (final Node node : rack.getNodes()) {
                                if (!workingNodes.contains(node.getId())) {
                                    for (final DataBlock dataBlock : node.getDataBlocks()) {
                                        if (dataBlockId.equals(dataBlock.getId())) {
                                            return Optional.of(new MapReduceTask(taskId, node, dataBlock));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return Optional.absent();
                }
            }));
        }

        for (final Future<Optional<MapReduceTask>> future : futures) {
            final Optional<MapReduceTask> mapReduceTask = Futures.getUnchecked(future);

            if (mapReduceTask.isPresent()) {
                tasks.add(mapReduceTask.get());
            }
        }

        return tasks;
    }
}
