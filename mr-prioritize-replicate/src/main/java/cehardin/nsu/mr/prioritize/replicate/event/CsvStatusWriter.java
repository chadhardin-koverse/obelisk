package cehardin.nsu.mr.prioritize.replicate.event;

import static java.util.Collections.unmodifiableList;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;


import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cehar_000
 */
public class CsvStatusWriter implements StatusWriter {
    private final List<String> header = unmodifiableList(newArrayList(
            "time", "timeStep", 
            "numNodes", "numFailedNodes", 
            "numDataBlocks", 
            "numMRTasks", 
            "numReplicaTasks", "numCriticalReplicaTasks", "numNonCriticalReplicaTasks",
            "numMRTasksLeft", 
            /*"numMRTasksKilled", "numReplicateTasksKilled", */
            "num0Replicas", "num1Replicas", "num2Replicas", "num3Replicas", "numOver3Replicas"));
    private final Writer writer;
    private boolean wroteHeader = false;

    
    public CsvStatusWriter(Writer writer) {
        this.writer = writer;
    }

    public void write(Status status) throws IOException {
        if(!wroteHeader) {
           write(header);
           wroteHeader = true;
        }
        write(toList(status));
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
    
    
    private List<String> toList(Status status) {
        final List<String> list = newArrayList();
        final Map<Integer, Integer> counts = newHashMap();
        
        list.add(Long.toString(status.getTime()));
        list.add(Long.toString(status.getTimeStep()));
        list.add(Integer.toString(status.getNumNodes()));
        list.add(Integer.toString(status.getNumFailedNodes()));
        list.add(Integer.toString(status.getNumDataBlocks()));
        list.add(Integer.toString(status.getNumMRTasks()));
        list.add(Integer.toString(status.getNumReplicaTasks()));
        list.add(Integer.toString(status.getNumCriticalReplicaTasks()));
        list.add(Integer.toString(status.getNumNonCriticalReplicTasks()));
        list.add(Integer.toString(status.getNumMRTasksLeft()));
        //list.add(Integer.toString(status.getNumMRTasksKilled()));
        //list.add(Integer.toString(status.getNumReplicateTasksKilled()));
        
        counts.put(0, 0);
        counts.put(1, 0);
        counts.put(2, 0);
        counts.put(3, 0);
        counts.put(4, 0);
        
        for(final Integer count : status.getReplicaCount().values()) {
            final int key = count > 3 ? 4 : count;
            
            counts.put(key, counts.get(key) + 1);
        } 

        list.add(Integer.toString(counts.get(0)));
        list.add(Integer.toString(counts.get(1)));
        list.add(Integer.toString(counts.get(2)));
        list.add(Integer.toString(counts.get(3)));
        list.add(Integer.toString(counts.get(4)));
        
        return list;
    }

    private void write(Iterable<?> iterable) throws IOException {
       write(iterable.iterator());
    }
    
    private void write(Iterator<?> iterator) throws IOException {
        while (iterator.hasNext()) {
           final Object next = iterator.next();
           writer.append(next == null ? "" : next.toString());
           if (iterator.hasNext()) {
               writer.append(',');
           }
        }
        writer.append("\r\n");
    }
}
