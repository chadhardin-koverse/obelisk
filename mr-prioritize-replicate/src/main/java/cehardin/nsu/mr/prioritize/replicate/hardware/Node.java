package cehardin.nsu.mr.prioritize.replicate.hardware;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Iterables.transform;
import static cehardin.nsu.mr.prioritize.replicate.DataBlock.extractIdFromDataBlock;
import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Chad
 */
public class Node extends AbstractHardware<NodeId> {

    private static class ExtractDataBlocks implements Function<Node, Iterable<DataBlock>> {
        public Iterable<DataBlock> apply(Node node) {
            return node.getDataBlocks();
        }
    }
    
    private static class ContainsDataBlock implements Predicate<Node> {
        private final DataBlockId dataBlockId;

        public ContainsDataBlock(final DataBlockId dataBlockId1) {
            this.dataBlockId = dataBlockId1;
        }
        public boolean apply(Node node) {
            return node.getDataBlockIds().contains(dataBlockId);
        }
    }
    
    private static Function<Node, Iterable<DataBlock>> ExtractDataBlocks = new ExtractDataBlocks();

    public static Function<Node, Iterable<DataBlock>> extractDataBlocksFromNode() {
        return ExtractDataBlocks;
    }
    
    public static Predicate<Node> nodeContainsDataBlock(final DataBlockId dataBlockId) {
        return new ContainsDataBlock(dataBlockId);
    }
    
    private final Resource diskResource;
    private final Set<DataBlock> dataBlocks;

    public Node(NodeId id, Resource diskResource, Iterable<DataBlock> dataBlocks) {
        super(id);
        this.diskResource = diskResource;
        this.dataBlocks = newHashSet(dataBlocks);
    }

    public Resource getDiskResource() {
        return diskResource;
    }

    public Set<DataBlock> getDataBlocks() {
        return dataBlocks;
    }
    
    public Set<DataBlockId> getDataBlockIds() {
        return newHashSet(transform(getDataBlocks(), extractIdFromDataBlock()));
    }

    public Map<DataBlockId, DataBlock> getDataBlockById() {
        return uniqueIndex(getDataBlocks(), extractIdFromDataBlock());
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).
                add("id", getId()).
                add("dataBlocks", dataBlocks).
                add("diskResource", diskResource).
                toString();
    }
}
