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
public interface Hardware<ID extends Id> {

    ID getId();
}
