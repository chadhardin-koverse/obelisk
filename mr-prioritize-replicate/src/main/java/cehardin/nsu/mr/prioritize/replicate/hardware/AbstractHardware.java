/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.id.Id;

/**
 *
 * @author Chad
 */
public abstract class AbstractHardware<ID extends Id> implements Hardware<ID> {
	private final ID id;

	public AbstractHardware(ID id) {
		this.id = id;
	}

	public ID getId() {
		return id;
	}
}
