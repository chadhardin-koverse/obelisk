/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk;

import java.net.URL;
import java.util.Set;

/**
 *
 * @author Chad
 */
public interface MobileAgentCreator {
	void createMobileAgent(Class<?> agentClass, Set<URL> codeLocations, Object... arguments);
}
