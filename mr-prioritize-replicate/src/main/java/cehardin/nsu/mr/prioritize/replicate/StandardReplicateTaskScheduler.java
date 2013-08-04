package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.unmodifiableList;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.extractDataBlockIdFromReplicateTask;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import cehardin.nsu.mr.prioritize.replicate.task.Task;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class StandardReplicateTaskScheduler implements ReplicateTaskScheduler {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private final Random random;
    private final ExecutorService executorService;

    public StandardReplicateTaskScheduler(Random random, ExecutorService executorService) {
        this.random = random;
        this.executorService = executorService;
    }

    @Override
    public List<ReplicateTask> schedule(final Cluster cluster, final int maxTasks, final Iterable<ReplicateTask> runningTasks) {
        final List<ReplicateTask> tasks = newArrayList();
        final Set<DataBlockId> workingDataBlocks = newHashSet(transform(runningTasks, extractDataBlockIdFromReplicateTask()));
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
