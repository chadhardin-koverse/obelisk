/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

/**
 *
 * @author Chad
 */
public interface Simulated {

    /**
     *
     * @param availableTimeInMs The amount of time available in ms
     * @return Time used (ms)
     */
    double execute(double availableTimeInMs);
}
