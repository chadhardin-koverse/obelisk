package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.isReplicateTaskCritical;
import static java.util.Collections.reverseOrder;
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
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author Chad
 */
public class HotDataBlockReplicateTaskScheduler extends AbstractReplicateTaskScheduler {
    
    private final SortedMap<Double, Set<DataBlockId>> tempToDataBlockIds;
    private final Map<DataBlockId, Double> dataBlockIdToTemp;
    
    private final Comparator<ReplicateTask> comparator = new Comparator<ReplicateTask>() {

        @Override
        public int compare(ReplicateTask task1, ReplicateTask task2) {
            final double temp1 = dataBlockIdToTemp.get(task1.getDataBlock().getId());
            final double temp2 = dataBlockIdToTemp.get(task2.getDataBlock().getId());
            final boolean task1Critical = isReplicateTaskCritical().apply(task1);
            final boolean task2Critical = isReplicateTaskCritical().apply(task2);
            final int result;    
            
            if( (task1Critical && task2Critical) || (!task1Critical && !task2Critical)) {
                if(temp1 > temp2) {
                    result = 1;
                }
                else if(temp1 < temp2) {
                    result = -1;
                }
                else {
                    result = 0;
                }
            }
            else if(task1Critical) {
                result = 1;
            }
            else {
                result = -1;
            }
            
            return result;
        }
    };

    public HotDataBlockReplicateTaskScheduler(Random random, ExecutorService executorService, Map<DataBlockId, Double> dataBlockIdToTemp) {
        super(random, executorService);
        final Map<Double, Integer> temperatureCount = newHashMap();
        
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
        
        for(final Double temperatue : dataBlockIdToTemp.values()) {
            if(temperatureCount.containsKey(temperatue)) {
                temperatureCount.put(temperatue, temperatureCount.get(temperatue) + 1);
            }
            else {
                temperatureCount.put(temperatue, 1);
            }
        }
        
        System.out.println(String.format("Temperature Count: %s", new TreeMap<>(temperatureCount)));
        
    }

    @Override
    protected void sort(List<ReplicateTask> tasks) {
        Collections.sort(tasks, reverseOrder(comparator));
    }
}
