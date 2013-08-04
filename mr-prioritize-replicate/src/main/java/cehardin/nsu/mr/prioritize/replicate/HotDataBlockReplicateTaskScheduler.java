package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractDataBlockIdFromReplicateTask;
import com.google.common.base.Optional;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 * @author Chad
 */
public class HotDataBlockReplicateTaskScheduler extends AbstractReplicateTaskScheduler {
    
    private final SortedMap<Double, Set<DataBlockId>> tempToDataBlockIds;
    private final Map<DataBlockId, Double> dataBlockIdToTemp;
    private final Comparator<DataBlockId> comparator = new Comparator<DataBlockId>() {
            @Override
            public int compare(DataBlockId db1, DataBlockId db2) {
                final double t1 = dataBlockIdToTemp.get(db1);
                final double t2 = dataBlockIdToTemp.get(db2);

                return t1 > t2 ? 1 : t2 > t1 ? -1 : 0;
            }
        };

    public HotDataBlockReplicateTaskScheduler(Random random, ExecutorService executorService, Map<DataBlockId, Double> dataBlockIdToTemp) {
        super(random, executorService);
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

    @Override
    protected void sort(List<DataBlockId> oneCopy, List<DataBlockId> twoCopies) {
        Collections.sort(oneCopy, comparator);
        Collections.sort(twoCopies, comparator);
    }
}
