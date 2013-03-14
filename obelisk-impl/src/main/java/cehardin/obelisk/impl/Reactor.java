/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk.impl;

import cehardin.obelisk.Agent;

/**
 *
 * @author Chad
 */
public interface Reactor {
	void feed(Agent.Id from, Agent.Id to, Object message);
	
	void add(Agent.Id id, Agent agent);
	
	void remove(Agent.Id id);
}
