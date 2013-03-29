/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.task.Task;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 *
 * @author Chad
 */
public class Resource implements Simulated {
	private final Logger logger;
	private final double capacityInMs;
	private final Queue<Reservation> reservations = new ConcurrentLinkedQueue<Reservation>();
	
	public Resource(final String name, final double capacityInMs) {
		this.capacityInMs = capacityInMs;
		logger = Logger.getLogger("Resource ("+name+")");
	}

	public double execute(final double timeInMs) {
		final Iterator<Reservation> reservationIterator = reservations.iterator();
		double timeUsed = 0;
		
		while(reservationIterator.hasNext()) {
			if(timeUsed >= timeInMs) {
				break;
			}
			final double timeAvailable = timeInMs - timeUsed;
			final double capacityAvailable = capacityInMs * timeAvailable;
			final Reservation reservation = reservationIterator.next();
			final double capacityUsed = reservation.use(capacityAvailable);
			
			timeUsed += (capacityUsed / capacityInMs);
			
			logger.info(String.format("Used %.2f bytes in %,dms: %s", capacityUsed, (long)timeUsed, reservation.getTask()));
			
			if(reservation.isDone()) {
				logger.info(String.format("Reservation of %,d bytes fot task is complete: %s", (long)reservation.getNeeded(), reservation.getTask()));
				reservationIterator.remove();
			}
		}
		
		return timeUsed;
	}
	
	public void consume(final Task task, final double amount) {
		final Reservation reservation = new Reservation(task, amount);
		reservations.add(reservation);
		reservation.waitTillDone();
	}
	
	private static class Reservation {
		private final Task task;
		private final double needed;
		private final CountDownLatch completionLatch;
		private double used;
		
		public Reservation(final Task task, final double needed) {
			this.task = task;
			this.needed = needed;
			this.used = 0;
			this.completionLatch = new CountDownLatch(1);
		}

		public Task getTask() {
			return task;
		}
		
		public void waitTillDone() {
			try {
				completionLatch.await();
			}
			catch(InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		
		public double getNeeded() {
			return needed;
		}

		public double getUsed() {
			return used;
		}
		
		public double getLeft() {
			return getNeeded()- getUsed();
		}
		
		public boolean isDone() {
			return getLeft() <= 0;
		}
		
		public double use(final double available) {
			final double taken = available <= getLeft() ? available : getLeft();
			
			used += taken;
			
			if(isDone()) {
				completionLatch.countDown();
			}
			
			return taken;
		}
	}
}
