package cehardin.nsu.mr.prioritize.replicate.event;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author cehar_000
 */
public interface StatusWriter extends Closeable {
    void write(Status status) throws IOException;
}
