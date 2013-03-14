/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk.message;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Chad
 */
public class CreatedMesage {
	private final List<Object> arguments;
	
	public CreatedMesage(Iterable<?> arguments) {
		this.arguments = new ArrayList<Object>(1);
		
		for(final Object argument : arguments) {
			this.arguments.add(argument);
		}
	}
	
	public List<Object> getArguments() {
		return arguments;
	}
}
