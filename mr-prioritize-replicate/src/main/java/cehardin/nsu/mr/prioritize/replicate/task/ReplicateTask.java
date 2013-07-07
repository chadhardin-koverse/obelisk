package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import com.google.common.base.Objects;

/**
 *
 * @author Chad
 */
public class ReplicateTask implements Task {

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

    public void run() {
        final long size = dataBlock.getSize();

        fromNode.getDiskResource().consume(this, size);
        fromRack.getNetworkResource().consume(this, size);
        if (fromRack.equals(toRack)) {
            toNode.getDiskResource().consume(this, size);
        } else {
            cluster.getNetworkResource().consume(this, size);
            toRack.getNetworkResource().consume(this, size);
            toNode.getDiskResource().consume(this, size);
        }
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
