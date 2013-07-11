package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 *
 * @author Chad
 */
public final class MapReduceTask implements Task {

    private final Node node;
    private final DataBlock dataBlock;

    public MapReduceTask(Node node, DataBlock dataBlock) {
        this.node = node;
        this.dataBlock = dataBlock;
    }

    public void run(Runnable callback) {
        Preconditions.checkState(node.getDataBlocks().contains(dataBlock));

        node.getDiskResource().consume(dataBlock.getSize(), callback);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).
                add("node", node.getId()).
                add("dataBlock", dataBlock.getId()).
                toString();
    }
}
