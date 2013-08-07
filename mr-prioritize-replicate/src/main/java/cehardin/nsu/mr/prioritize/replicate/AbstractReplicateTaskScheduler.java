package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newConcurrentMap;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractDataBlockIdFromReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractNodesFromReplicateTask;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newCopyOnWriteArraySet;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 * @author Chad
 */
public abstract class AbstractReplicateTaskScheduler implements ReplicateTaskScheduler {

    private final Random random;
    private final ExecutorService executorService;

    protected AbstractReplicateTaskScheduler(Random random, ExecutorService executorService) {
        this.random = random;
        this.executorService = executorService;
    }

    @Override
    public final List<ReplicateTask> schedule(final Cluster cluster, final Iterable<ReplicateTask> runningTasks) {
        final List<ReplicateTask> tasks = newArrayList();
        final Set<DataBlockId> workingDataBlocks = newHashSet(transform(runningTasks, extractDataBlockIdFromReplicateTask()));
        final Set<Node> workingNodes = newCopyOnWriteArraySet(concat(transform(runningTasks, extractNodesFromReplicateTask())));
        final List<DataBlockId> one = new ArrayList<>();
        final List<DataBlockId> two = new ArrayList<>();
        final List<Future<Optional<ReplicateTask>>> futures = new ArrayList<>();
        final ConcurrentMap<DataBlockId, ConcurrentMap<NodeId, ConcurrentMap<RackId, DataBlock>>> reverseTopology = newConcurrentMap();
        final CountDownLatch reverseTopologyCountdownLatch = new CountDownLatch(cluster.getRacks().size());
        final ConcurrentMap<RackId, Rack> racks = newConcurrentMap();
        final ConcurrentMap<NodeId, Node> nodes = newConcurrentMap();

        for (final Map.Entry<DataBlockId, Integer> countEntry : cluster.getDataBlockCount().entrySet()) {
            final DataBlockId dataBlockId = countEntry.getKey();
            final int count = countEntry.getValue();

            if (count < 3 && !workingDataBlocks.contains(dataBlockId)) {
                if (count == 1) {
                    one.add(dataBlockId);
                } else {
                    two.add(dataBlockId);
                }
            }
        }

        for (final Rack rack : cluster.getRacks()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    final RackId rackId = rack.getId();
                    racks.putIfAbsent(rackId, rack);
                    for (final Node node : rack.getNodes()) {
                        final NodeId nodeId = node.getId();
                        nodes.putIfAbsent(nodeId, node);
                        if (!workingNodes.contains(node)) {
                            for (final DataBlock dataBlock : node.getDataBlocks()) {
                                final DataBlockId dataBlockId = dataBlock.getId();
                                if (!workingDataBlocks.contains(dataBlockId)) {
                                    if (one.contains(dataBlockId) || two.contains(dataBlockId)) {
                                        reverseTopology.putIfAbsent(dataBlockId, new ConcurrentHashMap<NodeId, ConcurrentMap<RackId, DataBlock>>());
                                        reverseTopology.get(dataBlockId).putIfAbsent(nodeId, new ConcurrentHashMap<RackId, DataBlock>());
                                        reverseTopology.get(dataBlockId).get(nodeId).put(rackId, dataBlock);
                                    }
                                }
                            }
                        }
                    }
                    reverseTopologyCountdownLatch.countDown();
                }
            });

        }
        try {
            reverseTopologyCountdownLatch.await();
        }
        catch(final Throwable t) {
            throw Throwables.propagate(t);
        }
        
        sort(one, two);

        for (final DataBlockId dataBlockId : one) {
            futures.add(executorService.submit(new Callable<Optional<ReplicateTask>>() {
                @Override
                public Optional<ReplicateTask> call() throws Exception {
                    if (reverseTopology.containsKey(dataBlockId)) {
                        for (final Map.Entry<NodeId, ConcurrentMap<RackId, DataBlock>> nodeEntry : reverseTopology.get(dataBlockId).entrySet()) {
                            final NodeId nodeId = nodeEntry.getKey();
                            for (final Map.Entry<RackId, DataBlock> rackEntry : nodeEntry.getValue().entrySet()) {
                                final RackId rackId = rackEntry.getKey();
                                final DataBlock dataBlock = rackEntry.getValue();
                                final Rack rack = racks.get(rackId);
                                final Node fromNode = nodes.get(nodeId);
                                if (!workingNodes.contains(fromNode)) {
                                    for (final Node toNode : rack.getNodes()) {
                                        if (!toNode.equals(fromNode)) {
                                            workingNodes.add(fromNode);
                                            return Optional.of(new ReplicateTask(dataBlock, cluster, rack, rack, fromNode, toNode));
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



        for (final DataBlockId dataBlockId : two) {
            futures.add(executorService.submit(new Callable<Optional<ReplicateTask>>() {
                @Override
                public Optional<ReplicateTask> call() throws Exception {
                    if (reverseTopology.containsKey(dataBlockId)) {
                        for (final Map.Entry<NodeId, ConcurrentMap<RackId, DataBlock>> nodeEntry : reverseTopology.get(dataBlockId).entrySet()) {
                            final NodeId nodeId = nodeEntry.getKey();
                            for (final Map.Entry<RackId, DataBlock> rackEntry : nodeEntry.getValue().entrySet()) {
                                final RackId rackId = rackEntry.getKey();
                                final DataBlock dataBlock = rackEntry.getValue();
                                final Rack fromRack = racks.get(rackId);
                                final Node fromNode = nodes.get(nodeId);
                                if (!workingNodes.contains(fromNode)) {
                                    for (final Rack toRack : cluster.getRacks()) {
                                        if (!fromRack.equals(toRack)) {
                                            for (final Node toNode : toRack.getNodes()) {
                                                if (!toNode.getDataBlocks().contains(dataBlock)) {
                                                    workingNodes.add(fromNode);
                                                    return Optional.of(new ReplicateTask(dataBlock, cluster, fromRack, toRack, fromNode, toNode));
                                                }
                                            }
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

        for (final Future<Optional<ReplicateTask>> future : futures) {
            final Optional<ReplicateTask> replicateTask = Futures.getUnchecked(future);

            if (replicateTask.isPresent()) {
                tasks.add(replicateTask.get());
            }
        }

        return Collections.unmodifiableList(tasks);
    }

    protected abstract void sort(List<DataBlockId> oneCopy, List<DataBlockId> twoCopies);
}
