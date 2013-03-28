/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author Chad
 */
public class Resource implements Simulated {
	private final double capacityInMs;
	private final Queue<Reservation> reservations = new LinkedList<Reservation>();
	
	public Resource(final double capacityInMs) {
		this.capacityInMs = capacityInMs;
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
			
			if(reservation.isDone()) {
				reservationIterator.remove();
			}
		}
		
		return timeUsed;
	}
	
	public void consume(final double amount) {
		final Reservation reservation = new Reservation(amount);
		reservations.add(reservation);
		reservation.waitTillDone();
	}
	
	private static class Reservation {
		private final double needed;
		private final CountDownLatch completionLatch;
		private double used;
		
		public Reservation(final double needed) {
			this.needed = needed;
			this.used = 0;
			this.completionLatch = new CountDownLatch(1);
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
