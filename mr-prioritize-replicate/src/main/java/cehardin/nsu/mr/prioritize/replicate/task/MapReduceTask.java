package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import sun.rmi.log.ReliableLog;

/**
 *
 * @author Chad
 */
public final class MapReduceTask implements Task {

    private static class ReliesOnNode implements Predicate<MapReduceTask> {
        private final Node node;
        
        public ReliesOnNode(final Node node) {
            this.node = node;
        }
        
        public boolean apply(final MapReduceTask mapReduceTask) {
            return mapReduceTask.node.equals(node);
        }
    }
    
    public static Predicate<MapReduceTask> mapReduceTaskReliesOnNode(final Node node) {
        return new ReliesOnNode(node);
    }

    private final TaskId taskId;
    private final Node node;
    private final DataBlock dataBlock;

    public MapReduceTask(TaskId taskId, Node node, DataBlock dataBlock) {
        this.taskId = taskId;
        this.node = node;
        this.dataBlock = dataBlock;
    }

    public Node getNode() {
        return node;
    }

    public DataBlock getDataBlock() {
        return dataBlock;
    }

    public TaskId getTaskId() {
        return taskId;
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
