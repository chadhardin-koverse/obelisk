package cehardin.nsu.mr.prioritize.replicate.event;

import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author cehar_000
 */
public interface EventWriter {
    void write(Event event);
}
