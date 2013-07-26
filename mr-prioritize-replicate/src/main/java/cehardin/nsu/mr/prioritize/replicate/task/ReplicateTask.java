package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;

/**
 *
 * @author Chad
 */
public class ReplicateTask implements Task {
    private static class SameSource implements Predicate<Task> {
        private final RackId fromRackId;
        private final NodeId fromNodeId;
        private final DataBlockId dataBlockId;

        public SameSource(RackId fromRackId, NodeId fromNodeId, DataBlockId dataBlockId) {
            this.fromRackId = fromRackId;
            this.fromNodeId = fromNodeId;
            this.dataBlockId = dataBlockId;
        }

        @Override
        public boolean apply(Task task) {
            if(ReplicateTask.class.isInstance(task)) {
                final ReplicateTask replicateTask = ReplicateTask.class.cast(task);
                return Objects.equal(dataBlockId, replicateTask.dataBlock.getId());
            }
            else {
                return false;
            }
        }
    }
    
    public static Predicate<Task> replicateTaskSameSource(final ReplicateTask replicateTask) {
        return new SameSource(replicateTask.fromRack.getId(), replicateTask.fromNode.getId(), replicateTask.dataBlock.getId());
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
                fromRack.getNetworkResource().consume(size, new Runnable() {
                    public void run() {
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
                                    toRack.getNetworkResource().consume(size, new Runnable() {
                                        public void run() {
                                            toNode.getDiskResource().consume(size, new Runnable() {
                                                public void run() {
                                                    toNode.getDataBlocks().add(dataBlock);
                                                    callback.run();
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    }
                });
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
        
        if(this == o) {
            equal = true;
        }
        else if(getClass().isInstance(o)) {
            final ReplicateTask other = getClass().cast(o);
            equal = Objects.equal(dataBlock, other.dataBlock) &&
                    Objects.equal(fromNode, other.fromNode) &&
                    Objects.equal(toNode, other.toNode) &&
                    Objects.equal(fromRack, other.fromRack) &&
                    Objects.equal(toRack, other.toRack);
        }
        else {
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
