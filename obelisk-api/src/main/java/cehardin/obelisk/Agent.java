/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk;

import java.util.Date;

/**
 *
 * @author Chad
 */
public interface Agent {
	public static interface Id {
		
	};
	
	public static interface Context {
		Object getMessage();
		
		Id getSender();
		
		Id getId();
		
		Id getRootId();
		
		void stop();
		
		void send(Id destination, Object message);
		
		Id create(Class<? extends Agent> agentClass);
	}
	
	void execute(Context context);
}
