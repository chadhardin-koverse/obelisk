/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

/**
 *
 * @author Chad
 */
public abstract class AbstractHardware implements Hardware {
	private final String name;

	public AbstractHardware(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
