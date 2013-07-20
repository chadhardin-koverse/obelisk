package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.base.Objects;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class Resource {

    private final Logger logger;
    private final double capacityInMs;
    private final Queue<Reservation> reservations = new ConcurrentLinkedQueue<Reservation>();

    public Resource(final String name, final double capacityInMs) {
        this.capacityInMs = capacityInMs;
        logger = Logger.getLogger("Resource (" + name + ")");
    }

    /**
     *
     * @param availableTimeInMs The amount of time available in ms
     * @return Time used (ms)
     */
    public double execute(final double timeInMs) {
        final Iterator<Reservation> reservationIterator = reservations.iterator();
        double timeUsed = 0;

        while (reservationIterator.hasNext()) {
            if (timeUsed >= timeInMs) {
                break;
            }
            final double timeAvailable = timeInMs - timeUsed;
            final double capacityAvailable = capacityInMs * timeAvailable;
            final Reservation reservation = reservationIterator.next();
            final double capacityUsed = reservation.use(capacityAvailable);

            timeUsed += (capacityUsed / capacityInMs);

//            logger.info(String.format("Used %.2f bytes in %,dms: %s", capacityUsed, (long) timeUsed, reservation));

            if (reservation.isDone()) {
                reservation.getCallback().run();
//                logger.info(String.format("Reservation is complete: %s", reservation));
                reservationIterator.remove();
            }
        }

        return timeUsed;
    }

    public void consume(final double amount, final Runnable callback) {
        final Reservation reservation = new Reservation(callback, amount);
        reservations.add(reservation);
    }

    private static class Reservation {

        private final Runnable callback;
        private final double needed;
        private double used;

        public Reservation(final Runnable callback, final double needed) {
            this.callback = callback;
            this.needed = needed;
            this.used = 0;
        }

        public Runnable getCallback() {
            return callback;
        }

        public double getNeeded() {
            return needed;
        }

        public double getUsed() {
            return used;
        }

        public double getLeft() {
            return getNeeded() - getUsed();
        }

        public boolean isDone() {
            return getLeft() <= 0;
        }

        public double use(final double available) {
            final double taken = available <= getLeft() ? available : getLeft();

            used += taken;

            return taken;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(getClass()).
                    add("needed", needed).
                    add("used", used).
                    add("callback", callback).
                    toString();
        }
    }
}
