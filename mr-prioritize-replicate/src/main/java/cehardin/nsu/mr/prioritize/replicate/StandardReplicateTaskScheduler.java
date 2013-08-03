package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class StandardReplicateTaskScheduler implements ReplicateTaskScheduler {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private final Random random;

    public StandardReplicateTaskScheduler(Random random) {
        this.random = random;
    }
    
    public List<ReplicateTask> schedule(Cluster cluster) {
        final List<ReplicateTask> tasks = newArrayList();

        for (final Map.Entry<DataBlockId, Integer> countEntry : cluster.getDataBlockCount().entrySet()) {
            final DataBlockId dataBlockId = countEntry.getKey();
            final int count = countEntry.getValue();
            final Set<Rack> racksContainingDataBlock = cluster.findRacksOfDataBlock(dataBlockId);
            final Rack fromRack = racksContainingDataBlock.iterator().next();
            final Node fromNode = fromRack.findNodesOfDataBlockId(dataBlockId).iterator().next();
            final DataBlock dataBlock = fromNode.getDataBlockById().get(dataBlockId);

            if (count < 3 && count > 0) {
                final Rack toRack;
                final Node toNode;

                if (count == 1) {
                    toRack = fromRack;
                } else {
                    toRack = Util.pickRandom(random, cluster.getRacks(), fromRack);
                }

                if (!toRack.getNodes().isEmpty()) {
                    toNode = Util.pickRandom(random, toRack.getNodes());

                    tasks.add(new ReplicateTask(dataBlock, cluster, fromRack, toRack, fromNode, toNode));
                }
            }
        }

        return unmodifiableList(tasks);
    }
}
