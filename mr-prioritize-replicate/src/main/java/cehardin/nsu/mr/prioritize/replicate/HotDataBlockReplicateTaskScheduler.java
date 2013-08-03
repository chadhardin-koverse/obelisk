package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author Chad
 */
public class HotDataBlockReplicateTaskScheduler implements ReplicateTaskScheduler {

    private final Random random;
    private final SortedMap<Double, Set<DataBlockId>> tempToDataBlockIds;
    private final Map<DataBlockId, Double> dataBlockIdToTemp;

    public HotDataBlockReplicateTaskScheduler(Random random, Map<DataBlockId, Double> dataBlockIdToTemp) {
        this.random = random;
        this.dataBlockIdToTemp = newHashMap(dataBlockIdToTemp);
        this.tempToDataBlockIds = newTreeMap();

        for (final Map.Entry<DataBlockId, Double> entry : dataBlockIdToTemp.entrySet()) {
            final DataBlockId dataBlockId = entry.getKey();
            final Double temp = entry.getValue();

            if (!tempToDataBlockIds.containsKey(temp)) {
                tempToDataBlockIds.put(temp, new HashSet<DataBlockId>());
            }

            tempToDataBlockIds.get(temp).add(dataBlockId);
        }
    }

    public List<ReplicateTask> schedule(Cluster cluster) {
        final List<ReplicateTask> tasks = newArrayList();

        for (final Map.Entry<Integer, Set<DataBlockId>> countEntry : cluster.getReplicationCounts().entrySet()) {
            final int count = countEntry.getKey();
            final SortedSet<DataBlockId> datablockIds = new TreeSet<DataBlockId>(new Comparator<DataBlockId>() {
                public int compare(DataBlockId db1, DataBlockId db2) {
                    final double t1 = dataBlockIdToTemp.get(db1);
                    final double t2 = dataBlockIdToTemp.get(db2);

                    return t1 > t2 ? 1 : t2 > t1 ? -1 : 0;
                }
            });

            datablockIds.addAll(countEntry.getValue());

            for (final DataBlockId dataBlockId : datablockIds) {
                final Set<Rack> racksContainingDataBlock = cluster.findRacksOfDataBlock(dataBlockId);
                final Rack fromRack = racksContainingDataBlock.iterator().next();
                final Node fromNode = fromRack.findNodesOfDataBlockId(dataBlockId).iterator().next();
                final DataBlock dataBlock = fromNode.getDataBlockById().get(dataBlockId);
                final Rack toRack;
                final Node toNode;

                if (count == 1) {
                    toRack = fromRack;
                } else {
                    toRack = Util.pickRandom(random, cluster.getRacks(), fromRack);
                }

                toNode = Util.pickRandom(random, toRack.getNodes());

                tasks.add(new ReplicateTask(dataBlock, cluster, fromRack, toRack, fromNode, toNode));
            }
        }

        return Collections.unmodifiableList(tasks);
    }
}
