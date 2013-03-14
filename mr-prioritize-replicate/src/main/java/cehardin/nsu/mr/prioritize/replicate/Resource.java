/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chad
 */
public class Resource {
	private final ScheduledExecutorService executorService;
	private final double capacityPerSecond;
	private volatile boolean busy = false;
	private final Queue<Reservation> reservations = new LinkedList<Reservation>();
	
	public Resource(ScheduledExecutorService executorService, double capacityPerSecond) {
		this.executorService = executorService;
		this.capacityPerSecond = capacityPerSecond;
	}
	
	public synchronized void consume(final double amount, final Runnable callback) {
		reservations.offer(new Reservation(amount, callback));
		schedule();
	}
	
	private synchronized void schedule() {
		if(!busy) {
			final Reservation reservation = reservations.poll();
			
			if(reservation != null) {
				final double delay = 1000.0 * reservation.getAmount() / capacityPerSecond;
				
				busy = true;
				executorService.schedule(new Runnable() {

					public void run() {
						synchronized(Resource.this) {
							busy = false;
							executorService.execute(reservation.getCallback());
							schedule();	
						}
					}
				}, (long)delay, TimeUnit.MILLISECONDS);
			}
		}
	}
	
	private static class Reservation {
		private final double amount;
		private final Runnable callback;

		public Reservation(double amount, Runnable callback) {
			this.amount = amount;
			this.callback = callback;
		}

		public double getAmount() {
			return amount;
		}

		public Runnable getCallback() {
			return callback;
		}
	}
}
