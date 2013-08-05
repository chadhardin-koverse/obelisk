package cehardin.nsu.mr.prioritize.replicate.task;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 *
 * @author Chad
 */
public class ReplicateTask implements Task {

    private static class SameBlock implements Predicate<Task> {

        private final DataBlockId dataBlockId;

        public SameBlock(DataBlockId dataBlockId) {
            this.dataBlockId = dataBlockId;
        }

        @Override
        public boolean apply(Task task) {
            if (ReplicateTask.class.isInstance(task)) {
                final ReplicateTask replicateTask = ReplicateTask.class.cast(task);
                return Objects.equal(dataBlockId, replicateTask.dataBlock.getId());
            } else {
                return false;
            }
        }
    }
    
    private static class ExtractDataBlockId implements Function<ReplicateTask, DataBlockId> {
        @Override
        public DataBlockId apply(ReplicateTask replicateTask) {
            return replicateTask.dataBlock.getId();
        }
    }
    
    private static class ExtractFromNode implements Function<ReplicateTask, Node> {
        @Override
        public Node apply(ReplicateTask replicateTask) {
            return replicateTask.fromNode;
        }
    }
    
    private static class ExtractToNode implements Function<ReplicateTask, Node> {
        @Override
        public Node apply(ReplicateTask replicateTask) {
            return replicateTask.toNode;
        }
    }
    
    private static class ExtractNodes implements Function<ReplicateTask, Iterable<Node>> {
        @Override
        public Iterable<Node> apply(ReplicateTask replicateTask) {
//            return unmodifiableList(newArrayList(replicateTask.fromNode, replicateTask.toNode));
            return unmodifiableList(newArrayList(replicateTask.fromNode));
        }
    }
    
    private static class ReliesOnNode implements Predicate<ReplicateTask> {
        private final Node node;
        
        public ReliesOnNode(final Node node) {
            this.node = node;
        }
        
        @Override
        public boolean apply(final ReplicateTask replicateTask) {
            return replicateTask.fromNode.equals(node) || replicateTask.toNode.equals(node);
        }
    }
    

    private static final ExtractDataBlockId EXTRACT_DATA_BLOCK_ID = new ExtractDataBlockId();
    private static final ExtractFromNode EXTRACT_FROM_NODE = new ExtractFromNode();
    private static final ExtractToNode EXTRACT_TO_NODE = new ExtractToNode();
    private static final ExtractNodes EXTRACT_NODES = new ExtractNodes();
    
    public static Predicate<Task> replicateTaskSameBlock(final ReplicateTask replicateTask) {
        return new SameBlock(replicateTask.dataBlock.getId());
    }
    
    public static Function<ReplicateTask, DataBlockId> extractDataBlockIdFromReplicateTask() {
        return EXTRACT_DATA_BLOCK_ID;
    }
    
    public static Function<ReplicateTask, Node> extractFromNodeFromReplicateTask() {
        return EXTRACT_FROM_NODE;
    }
    
    public static Function<ReplicateTask, Node> extractToNodeFromReplicateTask() {
        return EXTRACT_TO_NODE;
    }
    
    public static Function<ReplicateTask, Iterable<Node>> extractNodesFromReplicateTask() {
        return EXTRACT_NODES;
    }
    
    public static Predicate<ReplicateTask> replicateTaskReliesOnNode(final Node node) {
        return new ReliesOnNode(node);
    }
    
    private final DataBlock dataBlock;
    private final Cluster cluster;
    private final Rack fromRack;
    private final Rack toRack;
    private final Node fromNode;
    private final Node toNode;

    public ReplicateTask(
            DataBlock dataBlock,
            Cluster cluster,
            Rack fromRack,
            Rack toRack,
            Node fromNode,
            Node toNode) {
        this.dataBlock = dataBlock;
        this.cluster = cluster;
        this.fromRack = fromRack;
        this.toRack = toRack;
        this.fromNode = fromNode;
        this.toNode = toNode;
    }

    public void run(final Runnable callback) {
        final long size = dataBlock.getSize();

        fromNode.getDiskResource().consume(size, new Runnable() {
            public void run() {
                if (cluster.getNodesById().keySet().containsAll(Lists.newArrayList(fromNode.getId(), toNode.getId()))) {
                    fromRack.getNetworkResource().consume(size, new Runnable() {
                        public void run() {
                            if (cluster.getNodesById().keySet().containsAll(Lists.newArrayList(fromNode.getId(), toNode.getId()))) {
                                if (fromRack.equals(toRack)) {
                                    toNode.getDiskResource().consume(size, new Runnable() {
                                        public void run() {
                                            toNode.getDataBlocks().add(dataBlock);
                                            callback.run();
                                        }
                                    });
                                } else {
                                    cluster.getNetworkResource().consume(size, new Runnable() {
                                        public void run() {
                                            if (cluster.getNodesById().keySet().containsAll(Lists.newArrayList(fromNode.getId(), toNode.getId()))) {
                                                toRack.getNetworkResource().consume(size, new Runnable() {
                                                    public void run() {
                                                        if (cluster.getNodesById().keySet().containsAll(Lists.newArrayList(fromNode.getId(), toNode.getId()))) {
                                                            toNode.getDiskResource().consume(size, new Runnable() {
                                                                public void run() {
                                                                    toNode.getDataBlocks().add(dataBlock);
                                                                    callback.run();
                                                                }
                                                            });
                                                        } else {
                                                            callback.run();
                                                        }
                                                    }
                                                });
                                            } else {
                                                callback.run();
                                            }
                                        }
                                    });
                                }
                            } else {
                                callback.run();
                            }
                        }
                    });
                } else {
                    callback.run();
                }
            }
        });
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                dataBlock,
                fromNode,
                toNode,
                fromRack,
                toRack);
    }

    @Override
    public boolean equals(Object o) {
        final boolean equal;

        if (this == o) {
            equal = true;
        } else if (getClass().isInstance(o)) {
            final ReplicateTask other = getClass().cast(o);
            equal = Objects.equal(dataBlock, other.dataBlock)
                    && Objects.equal(fromNode, other.fromNode)
                    && Objects.equal(toNode, other.toNode)
                    && Objects.equal(fromRack, other.fromRack)
                    && Objects.equal(toRack, other.toRack);
        } else {
            equal = false;
        }

        return equal;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).
                add("fromRack", fromRack.getId()).
                add("fromNode", fromNode.getId()).
                add("toRack", toRack.getId()).
                add("toNode", toNode.getId()).
                add("dataBlock", dataBlock.getId()).
                toString();
    }
}
