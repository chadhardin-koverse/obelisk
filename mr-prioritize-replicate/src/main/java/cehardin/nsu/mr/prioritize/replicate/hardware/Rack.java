package cehardin.nsu.mr.prioritize.replicate.hardware;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static cehardin.nsu.mr.prioritize.replicate.hardware.Node.extractDataBlocksFromNode;
import static cehardin.nsu.mr.prioritize.replicate.hardware.Node.nodeContainsDataBlock;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.filter;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Collections.unmodifiableSortedMap;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class Rack extends AbstractHardware<RackId> {
    private static class ExtractDataBlocksFromRack implements Function<Rack, Set<DataBlock>> {
        public Set<DataBlock> apply(Rack rack) {
            return rack.getDataBlocks();
        }
    }
    
    private static class RackContainsNode implements Predicate<Rack> {
        private final NodeId nodeId;

        public RackContainsNode(NodeId nodeId) {
            this.nodeId = nodeId;
        }

        public boolean apply(Rack rack) {
            return rack.getNodesById().containsKey(nodeId);
        }   
    }
    
    private static final Function<Rack, Set<DataBlock>> ExtractDataBlocksFromRack = new ExtractDataBlocksFromRack();
    
    public static Function<Rack, Set<DataBlock>> extractDataBlocksFromRack() {
        return ExtractDataBlocksFromRack;
    }
    
    public static Predicate<Rack> rackContainsNode(final NodeId nodeId) {
        return new RackContainsNode(nodeId);
    }
    
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private final Set<Node> nodes;
    private final Resource networkResource;

    public Rack(Iterable<Node> nodes, Resource networkResource, RackId id) {
        super(id);
        this.nodes = newHashSet(nodes);
        this.networkResource = networkResource;
    }

    public Set<Node> getNodes() {
        return nodes;
    }

    public Map<NodeId, Node> getNodesById() {
        return uniqueIndex(nodes, extractIdFromHardware(NodeId.class));
    }

    public Resource getNetworkResource() {
        return networkResource;
    }

    public Set<DataBlock> getDataBlocks() {
        return newHashSet(concat(transform(getNodes(), extractDataBlocksFromNode())));
    }

    public Map<DataBlockId, Set<DataBlock>> getDataBlocksById() {
        final Map<DataBlockId, Set<DataBlock>> blocksById = newHashMap();

        for (final Node node : getNodes()) {
            for (final Map.Entry<DataBlockId, DataBlock> nodeBlocksById : node.getDataBlockById().entrySet()) {
                final DataBlockId id = nodeBlocksById.getKey();
                final DataBlock dataBlock = nodeBlocksById.getValue();

                if (!blocksById.containsKey(id)) {
                    blocksById.put(id, new HashSet<DataBlock>());
                }

                blocksById.get(id).add(dataBlock);
            }
        }

        return unmodifiableMap(blocksById);
    }
    
    public Map<DataBlockId, Set<NodeId>> getDataBlockIdToNodeIds() {
        final Map<DataBlockId, Set<NodeId>> dataBlocksToNodes = newHashMap();
        
        for(final Node node : nodes) {
            final NodeId nodeId = node.getId();
            for(final DataBlock dataBlock : node.getDataBlocks()) {
                final DataBlockId dataBlockId = dataBlock.getId();
                if(!dataBlocksToNodes.containsKey(dataBlockId)) {
                    dataBlocksToNodes.put(dataBlockId, new HashSet<NodeId>());
                }
                
                dataBlocksToNodes.get(dataBlockId).add(nodeId);
            }
        }
        
        return unmodifiableMap(dataBlocksToNodes);
    }

    public Map<DataBlockId, Integer> getDataBlockCount() {
        final Map<DataBlockId, Integer> result = newHashMap();

        for (final Node node : getNodes()) {
            for (final DataBlock dataBlock : node.getDataBlocks()) {
                final DataBlockId dataBlockId = dataBlock.getId();

                if (result.containsKey(dataBlockId)) {
                    result.put(dataBlockId, result.get(dataBlockId) + 1);
                } else {
                    result.put(dataBlockId, 1);
                }
            }
        }

        return unmodifiableMap(result);
    }

    public SortedMap<Integer, Set<DataBlockId>> getReplicationCounts() {
        final SortedMap<Integer, Set<DataBlockId>> result = newTreeMap();

        for (final Map.Entry<DataBlockId, Integer> entry : getDataBlockCount().entrySet()) {
            final DataBlockId dataBlockId = entry.getKey();
            final Integer count = entry.getValue();

            if (!result.containsKey(count)) {
                result.put(count, new HashSet<DataBlockId>());
            }

            result.get(count).add(dataBlockId);
        }

        return unmodifiableSortedMap(result);
    }

//    public Node pickRandomNode() {
//        final Random random = new Random();
//        final int offset = random.nextInt(getNodes().size());
//        final Iterator<Node> nodeIterator = getNodes().iterator();
//
//        for (int i = 0; i < offset; i++) {
//            nodeIterator.next();
//        }
//
//        return nodeIterator.next();
//    }
//
//    public Node pickRandomNodeNot(Node node) {
//        Node randomNode;
//        do {
//            randomNode = pickRandomNode();
//        } while (randomNode == node);
//
//        return randomNode;
//
//    }

    public Set<Node> findNodesOfDataBlockId(final DataBlockId dataBlockId) {
        return unmodifiableSet(filter(getNodes(), nodeContainsDataBlock(dataBlockId)));
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).
                add("nodes", nodes).
                add("networkResource", networkResource).
                toString();
    }
}
