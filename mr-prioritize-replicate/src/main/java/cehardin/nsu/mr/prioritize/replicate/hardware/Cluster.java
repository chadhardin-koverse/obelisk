/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

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
    
    public Map<DataBlockId, Set<NodeId>> getDataBlockIdToNodeIds() {
        final Map<DataBlockId, Set<NodeId>> dataBlocksToNodes = newHashMap();
        
        for(final Rack rack : racks) {
            for(final Map.Entry<DataBlockId, Set<NodeId>> entry : rack.getDataBlockIdToNodeIds().entrySet()) {
                final DataBlockId dataBlockId = entry.getKey();
                final Set<NodeId> nodeIds = entry.getValue();
                
                if(!dataBlocksToNodes.containsKey(dataBlockId)) {
                    dataBlocksToNodes.put(dataBlockId, new HashSet<NodeId>());
                }
                
                dataBlocksToNodes.get(dataBlockId).addAll(nodeIds);
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
        final Set<Rack> found = newHashSet();

        for (final Rack rack : getRacks()) {
            if(rack.getDataBlocksById().containsKey(dataBlockId)) {
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
