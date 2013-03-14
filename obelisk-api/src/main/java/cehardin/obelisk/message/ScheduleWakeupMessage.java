/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk.message;

import java.util.Date;

/**
 *
 * @author Chad
 */
public class ScheduleWakeupMessage {
	private final long wakeupTime;

	public ScheduleWakeupMessage(final Date wakeupDate) {
		this.wakeupTime = wakeupDate.getTime();
	}
	
	public ScheduleWakeupMessage(final long wakeupTime) {
		this.wakeupTime = wakeupTime;
	}
	
	public Date getWakeupDate() {
		return new Date(getWakeupTime());
	}
	
	public long getWakeupTime() {
		return wakeupTime;
	}
	
	
}
