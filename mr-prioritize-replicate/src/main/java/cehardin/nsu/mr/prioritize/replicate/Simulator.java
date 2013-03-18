/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 * @author Chad
 */
public class Simulator implements Callable<Object> {
	private final ExecutorService executorService;
	private final Collection<Simulated> simulateds = new ArrayList<Simulated>();

	public Object call() throws Exception {
		final double totalTime = 1000000;
		final double timeStep = 1000;
		double currentTime = 0;
		
		while(currentTime < totalTime) {
			final double availableStepTime = (currentTime + timeStep) > totalTime ? totalTime - currentTime : timeStep;
			final Collection<Future<Double>> futures = new ArrayList<Future<Double>>(simulateds.size());
			for(final Simulated simulated : simulateds) {
				futures.add(executorService.submit(new Callable<Double>() {
					public Double call() throws Exception {
						return simulated.execute(availableStepTime);
					}
				}));
			}
			
			for(final Future<Double> future : futures) {
				future.get();
			}
		}
		
	}
	
	
}
