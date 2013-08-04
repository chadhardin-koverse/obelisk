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
public class HotDataBlockReplicateTaskScheduler implements ReplicateTaskScheduler {

    private final Random random;
    private final SortedMap<Double, Set<DataBlockId>> tempToDataBlockIds;
    private final Map<DataBlockId, Double> dataBlockIdToTemp;
    private final ExecutorService executorService;

    public HotDataBlockReplicateTaskScheduler(Random random, ExecutorService executorService, Map<DataBlockId, Double> dataBlockIdToTemp) {
        this.random = random;
        this.executorService = executorService;
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
    public List<ReplicateTask> schedule(final Cluster cluster, final int maxTasks, final Iterable<ReplicateTask> runningTasks) {
        final List<ReplicateTask> tasks = newArrayList();
        final Set<DataBlockId> workingDataBlocks = newHashSet(transform(runningTasks, extractDataBlockIdFromReplicateTask()));
        final Comparator<DataBlockId> comparator = new Comparator<DataBlockId>() {
            @Override
            public int compare(DataBlockId db1, DataBlockId db2) {
                final double t1 = dataBlockIdToTemp.get(db1);
                final double t2 = dataBlockIdToTemp.get(db2);

                return t1 > t2 ? 1 : t2 > t1 ? -1 : 0;
            }
        };
        final List<DataBlockId> one = new ArrayList<>();
        final List<DataBlockId> two = new ArrayList<>();
        final List<Future<ReplicateTask>> oneFutures = new ArrayList<>();
        final List<Future<ReplicateTask>> twoFutures = new ArrayList<>();
        
        for (final Map.Entry<DataBlockId, Integer> countEntry : cluster.getDataBlockCount().entrySet()) {
            final DataBlockId dataBlockId = countEntry.getKey();
            final int count = countEntry.getValue();

            if (count < 3 && !workingDataBlocks.contains(dataBlockId)) {
                if (count == 1) {
                    one.add(dataBlockId);
                } else {
                    two.add(dataBlockId);
                }
            }
        }

        Collections.sort(one, comparator);
        Collections.sort(two, comparator);

        for (final DataBlockId dataBlockId : one) {
            oneFutures.add(executorService.submit(new Callable<ReplicateTask>() {
                @Override
                public ReplicateTask call() throws Exception {
                    final Set<Rack> racksContainingDataBlock = cluster.findRacksOfDataBlock(dataBlockId);
                    final Rack rack = racksContainingDataBlock.iterator().next();
                    final Node fromNode = rack.findNodesOfDataBlockId(dataBlockId).iterator().next();
                    final DataBlock dataBlock = fromNode.getDataBlockById().get(dataBlockId);
                    final Node toNode = Util.pickRandom(random, rack.getNodes(), fromNode);

                    return new ReplicateTask(dataBlock, cluster, rack, rack, fromNode, toNode);
                }
            }));
            
            if((oneFutures.size() + twoFutures.size()) >= maxTasks) {
                break;
            }
        }


        for (final DataBlockId dataBlockId : two) {
            twoFutures.add(executorService.submit(new Callable<ReplicateTask>() {
                @Override
                public ReplicateTask call() throws Exception {
                    final Set<Rack> racksContainingDataBlock = cluster.findRacksOfDataBlock(dataBlockId);
                    final Rack fromRack = racksContainingDataBlock.iterator().next();
                    final Node fromNode = fromRack.findNodesOfDataBlockId(dataBlockId).iterator().next();
                    final DataBlock dataBlock = fromNode.getDataBlockById().get(dataBlockId);
                    final Rack toRack = Util.pickRandom(random, cluster.getRacks(), fromRack);
                    final Node toNode = Util.pickRandom(random, toRack.getNodes());

                    return new ReplicateTask(dataBlock, cluster, fromRack, toRack, fromNode, toNode);
                }
            }));
            
            if((oneFutures.size() + twoFutures.size()) >= maxTasks) {
                break;
            }
        }

        for (final Future<ReplicateTask> oneFuture : oneFutures) {
            tasks.add(Futures.getUnchecked(oneFuture));
        }

        for (final Future<ReplicateTask> twoFuture : twoFutures) {
            tasks.add(Futures.getUnchecked(twoFuture));
        }

        return Collections.unmodifiableList(tasks);
    }
}
