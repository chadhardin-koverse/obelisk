package cehardin.nsu.mr.prioritize.replicate.event;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static com.google.common.collect.Lists.newArrayList;

import java.io.IOException;


import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author cehar_000
 */
public class CsvEventWriter implements StatusWriter {
    private final List<String> header = unmodifiableList(newArrayList("time", "numNodes", "numFailedNodes", "numDataBlocks", "numMRTasks", "numReplicaTasks", "num0Replicas", "num1Replicas", "num2Replicas", "num3Replicas", "numOver3Replicas"));
    private final AtomicBoolean wroteHeader = new AtomicBoolean(false);
    private final Writer writer;

    public CsvEventWriter(Writer writer) {
        this.writer = writer;
    }

    public void write(Status status) {
        try {
            if(wroteHeader.compareAndSet(false, true)) {
                write(header);
            }
            write(toList(event));
        } catch (Exception e) {
            throw new RuntimeException(format("Error writing %s", event), e);
        }
    }
    
    private List<String> toList(Event event) {
        return newArrayList(
                event.getTask().toString(),
                event.getType().toString(),
                event.getDate().toGMTString(),
                event.getNodeId().getValue(),
                event.getDataBlockId().getValue(),
                event.getFromNodeId().getValue(),
                event.getToNodeId().getValue(),
                event.getFromRackId().getValue(),
                event.getToRackId().getValue());
    }

    private void write(Iterable<?> iterable) throws IOException {
        try {
            write(iterable.iterator());
        } catch (Exception e) {
            throw new IOException(format("Error writing %s", iterable), e);
        }
    }
    
    private void write(Iterator<?> iterator) throws IOException {
        try {
            while (iterator.hasNext()) {
                final Object next = iterator.next();
                writer.append(next == null ? "" : next.toString());
                if (iterator.hasNext()) {
                    writer.append(',');
                }
            }
            writer.append("\r\n");
        } catch (Exception e) {
            throw new IOException(format("Error writing %s", iterator), e);
        }
    }
}
