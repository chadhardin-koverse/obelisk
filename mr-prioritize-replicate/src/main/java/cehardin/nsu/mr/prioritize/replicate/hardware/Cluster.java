/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Iterables.transform;
import static cehardin.nsu.mr.prioritize.replicate.hardware.Rack.extractDataBlocksFromRack;
import static cehardin.nsu.mr.prioritize.replicate.hardware.Rack.rackContainsNode;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSortedMap;
import static java.util.Collections.unmodifiableSet;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Chad
 */
public class Cluster {

    private final Set<Rack> racks;
    private final Resource networkResource;

    public Cluster(Iterable<Rack> racks, Resource networkResource) {
        this.racks = newHashSet(racks);
        this.networkResource = networkResource;
    }

    public Set<Rack> getRacks() {
        return racks;
    }

    public Resource getNetworkResource() {
        return networkResource;
    }

    public Set<DataBlock> getDataBlocks() {
        return newHashSet(concat(transform(racks, extractDataBlocksFromRack())));
    }

    public Map<DataBlockId, Set<DataBlock>> getDataBlocksById() {
        final Map<DataBlockId, Set<DataBlock>> blocksById = newHashMap();

        for (final Rack rack : getRacks()) {
            for (final Map.Entry<DataBlockId, Set<DataBlock>> nodeBlocksById : rack.getDataBlocksById().entrySet()) {
                final DataBlockId id = nodeBlocksById.getKey();
                final Set<DataBlock> dataBlocks = nodeBlocksById.getValue();

                if (blocksById.containsKey(id)) {
                    blocksById.get(id).addAll(dataBlocks);
                } else {
                    blocksById.put(id, newHashSet(dataBlocks));
                }
            }
        }

        return unmodifiableMap(blocksById);
    }

    public Map<DataBlockId, Set<NodeId>> getDataBlockIdToNodeIds(ExecutorService executorService) {
        final List<Future<Map<DataBlockId, Set<NodeId>>>> futures = newArrayList();
        final Map<DataBlockId, Set<NodeId>> dataBlocksToNodes = newHashMap();

        for (final Rack rack : racks) {
            futures.add(executorService.submit(new Callable<Map<DataBlockId, Set<NodeId>>>() {
                @Override
                public Map<DataBlockId, Set<NodeId>> call() throws Exception {
                    final Map<DataBlockId, Set<NodeId>> localDataBlocksToNodes = newHashMap();

                    for (final Map.Entry<DataBlockId, Set<NodeId>> entry : rack.getDataBlockIdToNodeIds().entrySet()) {
                        final DataBlockId dataBlockId = entry.getKey();
                        final Set<NodeId> nodeIds = entry.getValue();

                        if (!localDataBlocksToNodes.containsKey(dataBlockId)) {
                            localDataBlocksToNodes.put(dataBlockId, new HashSet<NodeId>());
                        }

                        localDataBlocksToNodes.get(dataBlockId).addAll(nodeIds);
                    }

                    return localDataBlocksToNodes;
                }
            }));
        }

        for (final Future<Map<DataBlockId, Set<NodeId>>> future : futures) {
            for (final Map.Entry<DataBlockId, Set<NodeId>> entry : Futures.getUnchecked(future).entrySet()) {
                if (!dataBlocksToNodes.containsKey(entry.getKey())) {
                    dataBlocksToNodes.put(entry.getKey(), new HashSet<NodeId>());
                }

                dataBlocksToNodes.get(entry.getKey()).addAll(entry.getValue());
            }
        }

        return unmodifiableMap(dataBlocksToNodes);
    }

    public Map<DataBlockId, Integer> getDataBlockCount() {
        final Map<DataBlockId, Integer> result = newHashMap();

        for (final Rack rack : getRacks()) {
            for (final Map.Entry<DataBlockId, Integer> entry : rack.getDataBlockCount().entrySet()) {
                final DataBlockId dataBlockId = entry.getKey();
                final Integer count = entry.getValue();

                if (result.containsKey(dataBlockId)) {
                    final Integer currentCount = result.get(dataBlockId);
                    result.put(dataBlockId, currentCount + count);
                } else {
                    result.put(dataBlockId, count);
                }
            }
        }

        return unmodifiableMap(result);
    }

    public Map<DataBlockId, Integer> getDataBlockCount(ExecutorService executorService, final Predicate<? super DataBlockId> dataBlockIdPredicate) {
        final Map<DataBlockId, Integer> result = newHashMap();
        final List<Future<Map<DataBlockId, Integer>>> futures = newArrayList();

        for (final Rack rack : getRacks()) {
            futures.add(executorService.submit(new Callable<Map<DataBlockId, Integer>>() {
                @Override
                public Map<DataBlockId, Integer> call() throws Exception {
                    final Map<DataBlockId, Integer> map = newHashMap();

                    for (final Map.Entry<DataBlockId, Integer> entry : rack.getDataBlockCount(dataBlockIdPredicate).entrySet()) {
                        final DataBlockId dataBlockId = entry.getKey();
                        final Integer count = entry.getValue();

                        if (map.containsKey(dataBlockId)) {
                            map.put(dataBlockId, map.get(dataBlockId) + count);
                        } else {
                            map.put(dataBlockId, count);
                        }
                    }

                    return map;
                }
            }));
        }

        for (final Future<Map<DataBlockId, Integer>> future : futures) {
            final Map<DataBlockId, Integer> map = Futures.getUnchecked(future);

            for (final Map.Entry<DataBlockId, Integer> entry : map.entrySet()) {
                final DataBlockId dataBlockId = entry.getKey();
                final int count = entry.getValue();

                if (result.containsKey(dataBlockId)) {
                    result.put(dataBlockId, result.get(dataBlockId) + count);
                } else {
                    result.put(dataBlockId, count);
                }
            }
        }

        return unmodifiableMap(result);
    }

    public SortedMap<Integer, Set<DataBlockId>> getReplicationCounts() {
        final SortedMap<Integer, Set<DataBlockId>> result = newTreeMap();

        for (final Map.Entry<DataBlockId, Integer> entry : getDataBlockCount().entrySet()) {
            final DataBlockId dataBlockId = entry.getKey();
            final int count = entry.getValue();

            if (!result.containsKey(count)) {
                result.put(count, new HashSet<DataBlockId>());
            }

            result.get(count).add(dataBlockId);
        }

        return unmodifiableSortedMap(result);
    }

    public Map<NodeId, Node> getNodesById() {
        final Map<NodeId, Node> nodeMap = newHashMap();

        for (final Rack rack : getRacks()) {
            nodeMap.putAll(rack.getNodesById());
        }

        return unmodifiableMap(nodeMap);
    }

//    public Rack pickRandomRack(Random random) {
//        final int offset = random.nextInt(getRacks().size());
//        final Iterator<Rack> rackIterator = getRacks().iterator();
//
//        for (int i = 0; i < offset; i++) {
//            rackIterator.next();
//        }
//
//        return rackIterator.next();
//    }
//
//    public Rack pickRandomNodeNot(Random random, Rack rack) {
//        Rack randomRack;
//        do {
//            randomRack = pickRandomRack(random);
//        } while (randomRack == rack);
//
//        return randomRack;
//    }
    public Rack findRackOfNode(final NodeId nodeId) {
        return find(getRacks(), rackContainsNode(nodeId));
    }

    public Set<Node> findNodesOfDataBlock(DataBlockId dataBlockId) {
        final Set<Node> found = newHashSet();

        for (final Rack rack : getRacks()) {
            found.addAll(rack.findNodesOfDataBlockId(dataBlockId));
        }

        return unmodifiableSet(found);
    }

    public Set<Rack> findRacksOfDataBlock(final DataBlockId dataBlockId) {
        final Set<Rack> found = Sets.newCopyOnWriteArraySet();

        for (final Rack rack : getRacks()) {
            if (rack.hasDataBlock(dataBlockId)) {
                found.add(rack);
            }
        }

        return unmodifiableSet(found);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).
                add("racks", racks).
                add("networkResource", networkResource).
                toString();
    }
}
