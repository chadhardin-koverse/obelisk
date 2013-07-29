package cehardin.nsu.mr.prioritize.replicate.event;

import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author cehar_000
 */
public class MultithreadedEventWriter implements StatusWriter {
    private static class WriterTask extends TimerTask {
        private final Queue<Event> queue;
        private final StatusWriter eventWriter;

        public WriterTask(Queue<Event> queue, StatusWriter eventWriter) {
            this.queue = queue;
            this.eventWriter = eventWriter;
        }
        
        @Override
        public void run() {
            while(!queue.isEmpty()) {
                eventWriter.write(queue.remove());
            }
        }
        
    }
    private final Queue<Event> queue = new ConcurrentLinkedQueue<Event>();

    public MultithreadedEventWriter(StatusWriter eventWriter, Timer timer, long period) {
        timer.scheduleAtFixedRate(new WriterTask(queue, eventWriter), period, period);
    }

    public void write(Event event) {
        queue.add(event);
    }
}
