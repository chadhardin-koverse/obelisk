/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

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
				reservation.getCallback().run();
				reservationIterator.remove();
			}
		}
		
		return timeUsed;
	}
	
	public void consume(final double amount, final Runnable callback) {
		reservations.add(new Reservation(callback, amount));
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
			return getNeeded()- getUsed();
		}
		
		public boolean isDone() {
			return getLeft() <= 0;
		}
		
		public double use(final double available) {
			final double taken = available <= getLeft() ? available : getLeft();
			
			used += taken;
			
			return taken;
		}
	}
}
