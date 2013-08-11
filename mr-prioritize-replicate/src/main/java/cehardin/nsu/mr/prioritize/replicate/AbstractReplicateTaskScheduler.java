package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Sets.union;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractDataBlockIdFromReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractNodesFromReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.isReplicateTaskCritical;
import static cehardin.nsu.mr.prioritize.replicate.hardware.AbstractHardware.extractIdFromHardware;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newCopyOnWriteArraySet;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
        final Set<DataBlockId> workingDataBlocks = newCopyOnWriteArraySet(transform(runningTasks, extractDataBlockIdFromReplicateTask()));
        final Set<NodeId> workingNodes = newCopyOnWriteArraySet(transform(concat(transform(filter(runningTasks, isReplicateTaskCritical()), extractNodesFromReplicateTask())), extractIdFromHardware(NodeId.class)));
        final List<DataBlockId> one = new CopyOnWriteArrayList<>();
        final List<DataBlockId> two = new CopyOnWriteArrayList<>();
        final ConcurrentMap<DataBlockId, ConcurrentMap<NodeId, ConcurrentMap<RackId, DataBlock>>> reverseTopology = newConcurrentMap();
        final ConcurrentMap<DataBlockId, ConcurrentMap<NodeId, RackId>> negatedReverseTopology = newConcurrentMap();
        final ConcurrentMap<RackId, Rack> racks = newConcurrentMap();
        final ConcurrentMap<NodeId, Node> nodes = newConcurrentMap();
        final Map<DataBlockId, Integer> dataBlockCount = cluster.getDataBlockCount(executorService, not(in((workingDataBlocks))));

        {
            final List<Future<?>> futures = newArrayList();

            for (final Map.Entry<DataBlockId, Integer> countEntry : dataBlockCount.entrySet()) {
                final DataBlockId dataBlockId = countEntry.getKey();
                final int count = countEntry.getValue();

                futures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (count == 1) {
                            if (!workingDataBlocks.contains(dataBlockId)) {
                                one.add(dataBlockId);
                                workingDataBlocks.add(dataBlockId);
                            }
                        }
                    }
                }));
            }

            for (final Future<?> future : futures) {
                Futures.getUnchecked(future);
            }
        }

        {
            final List<Future<?>> futures = newArrayList();

            for (final Map.Entry<DataBlockId, Integer> countEntry : dataBlockCount.entrySet()) {
                final DataBlockId dataBlockId = countEntry.getKey();
                final int count = countEntry.getValue();

                futures.add(executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (count == 2) {
                            if (!workingDataBlocks.contains(dataBlockId)) {
                                two.add(dataBlockId);
                            }
                        }
                    }
                }));
            }

            for (final Future<?> future : futures) {
                Futures.getUnchecked(future);
            }
        }

        {
            final CountDownLatch countdownLatch = new CountDownLatch(cluster.getRacks().size());

            for (final Rack rack : cluster.getRacks()) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        final RackId rackId = rack.getId();
                        racks.putIfAbsent(rackId, rack);
                        for (final Node node : rack.getNodes()) {
                            final NodeId nodeId = node.getId();
                            nodes.putIfAbsent(nodeId, node);
                            for (final DataBlock dataBlock : node.getDataBlocks()) {
                                final DataBlockId dataBlockId = dataBlock.getId();
                                reverseTopology.putIfAbsent(dataBlockId, new ConcurrentHashMap<NodeId, ConcurrentMap<RackId, DataBlock>>());
                                reverseTopology.get(dataBlockId).putIfAbsent(nodeId, new ConcurrentHashMap<RackId, DataBlock>());
                                reverseTopology.get(dataBlockId).get(nodeId).put(rackId, dataBlock);
                            }
                        }
                        countdownLatch.countDown();
                    }
                });
            }


            try {
                countdownLatch.await();
            } catch (final Throwable t) {
                throw Throwables.propagate(t);
            }
        }


        //EXPERIMENTAL TO SPEED UOP TWO DATA BLOCK COUNT CALCS
        {
            final Map<DataBlockId, Set<NodeId>> dataBlocksToNodes = cluster.getDataBlockIdToNodeIds(executorService);
            final CountDownLatch countdownLatch = new CountDownLatch(dataBlocksToNodes.size() * cluster.getRacks().size());

            for (final Map.Entry<DataBlockId, Set<NodeId>> dataBlockToNodes : cluster.getDataBlockIdToNodeIds(executorService).entrySet()) {
                final DataBlockId dataBlockId = dataBlockToNodes.getKey();
                final Set<NodeId> positiveNodes = dataBlockToNodes.getValue();

                for (final Rack rack : cluster.getRacks()) {
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            for (final Node node : rack.getNodes()) {
                                if (!positiveNodes.contains(node.getId())) {
                                    negatedReverseTopology.putIfAbsent(dataBlockId, new ConcurrentHashMap<NodeId, RackId>());
                                    negatedReverseTopology.get(dataBlockId).put(node.getId(), rack.getId());
                                }
                            }
                            countdownLatch.countDown();
                        }
                    });
                }
            }


            try {
                countdownLatch.await();
            } catch (final Throwable t) {
                throw Throwables.propagate(t);
            }
        }

        {
            final List<Future<Optional<ReplicateTask>>> futures = new ArrayList<>();

            for (final DataBlockId dataBlockId : one) {
                futures.add(executorService.submit(new Callable<Optional<ReplicateTask>>() {
                    @Override
                    public Optional<ReplicateTask> call() throws Exception {
                        for (final Map.Entry<NodeId, ConcurrentMap<RackId, DataBlock>> nodeEntry : reverseTopology.get(dataBlockId).entrySet()) {
                            final NodeId nodeId = nodeEntry.getKey();
                            for (final Map.Entry<RackId, DataBlock> rackEntry : nodeEntry.getValue().entrySet()) {
                                final RackId rackId = rackEntry.getKey();
                                final DataBlock dataBlock = rackEntry.getValue();
                                final Rack rack = racks.get(rackId);
                                final Node fromNode = nodes.get(nodeId);

                                if (rack.getNodes().size() == 1) {
                                    two.add(dataBlockId);
                                } else {
                                    for (final Node toNode : rack.getNodes()) {
                                        if (!toNode.equals(fromNode)) {
                                            workingNodes.add(fromNode.getId());
                                            workingNodes.add(toNode.getId());
                                            return Optional.of(new ReplicateTask(dataBlock, cluster, rack, rack, fromNode, toNode));
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
        }

        {
            final List<Future<Optional<ReplicateTask>>> futures = new ArrayList<>();

            for (final DataBlockId dataBlockId : two) {
                futures.add(executorService.submit(new Callable<Optional<ReplicateTask>>() {
                    @Override
                    public Optional<ReplicateTask> call() throws Exception {
                        for (final Map.Entry<NodeId, ConcurrentMap<RackId, DataBlock>> nodeEntry : reverseTopology.get(dataBlockId).entrySet()) {
                            final NodeId nodeId = nodeEntry.getKey();
                            for (final Map.Entry<RackId, DataBlock> rackEntry : nodeEntry.getValue().entrySet()) {
                                final RackId rackId = rackEntry.getKey();
                                final DataBlock dataBlock = rackEntry.getValue();
                                final Rack fromRack = racks.get(rackId);
                                final Node fromNode = nodes.get(nodeId);
                                if (!workingNodes.contains(fromNode.getId())) {
                                    for(final NodeId toNode : negatedReverseTopology.get(dataBlockId).keySet()) {
                                        if(!workingNodes.contains(toNode)) {
                                            final RackId toRack = negatedReverseTopology.get(dataBlockId).get(toNode);
                                            return Optional.of(new ReplicateTask(dataBlock, cluster, fromRack, racks.get(toRack), fromNode, nodes.get(toNode)));
                                        }
                                    }
//                                    for (final Rack toRaffck : cluster.getRacks()) {
//                                        if (!fromRack.equals(toRack)) {
//                                            for (final Node toNode : toRack.getNodes()) {
//                                                if (!workingNodes.contains(toNode.getId())) {
//                                                    if (!toNode.getDataBlocks().contains(dataBlock)) {
//                                                        return Optional.of(new ReplicateTask(dataBlock, cluster, fromRack, toRack, fromNode, toNode));
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
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
        }

        sort(tasks);

        return Collections.unmodifiableList(tasks);
    }

    protected abstract void sort(List<ReplicateTask> tasks);
}
