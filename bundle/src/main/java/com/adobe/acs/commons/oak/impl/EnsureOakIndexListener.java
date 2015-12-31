package com.adobe.acs.commons.oak.impl;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import org.apache.sling.commons.scheduler.ScheduleOptions;
import org.apache.sling.commons.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This event listener will run EnsureOakIndexJobHandler as sling job when Ensure Definitions are updated.
 * @author hsaginor
 *
 */
public class EnsureOakIndexListener implements EventListener {

	private static final Logger log = LoggerFactory.getLogger(EnsureOakIndexListener.class);
	
	private final EnsureOakIndex ensureIndex;
	private final String oakIndexPath;
	private final String ensureDefinitionsPath;
	
	EnsureOakIndexListener(@Nonnull EnsureOakIndex ensureIndex, @Nonnull String oakIndexPath, @Nonnull String ensureDefinitionsPath) {
		this.ensureIndex = ensureIndex;
		this.oakIndexPath = oakIndexPath; 
		this.ensureDefinitionsPath = ensureDefinitionsPath;
	}
	
	@Override
	public void onEvent(EventIterator events) {
		logEvents(events);
		executeJob();
	}

	void executeJob() {
		// Currently EnsureOakIndexJobHandler is implemented to process entire Ensure definitions tree.
		// Might be able to optimize this by implementing a job that will process specific changes only. 
		EnsureOakIndexJobHandler.executeJob(ensureIndex, oakIndexPath, ensureDefinitionsPath);
	}
	
	private void logEvents(EventIterator events) {
		if(log.isDebugEnabled()) {
			while(events.hasNext()) {
				Event e = events.nextEvent();
				try {
					log.debug("Recieved JCR event " + getEventTypeFromEvent(e) + " for " + e.getPath());
				} catch (RepositoryException e1) {}
			}
		}
	}
	
	private String getEventTypeFromEvent(Event e) {
		String type = "";
		switch(e.getType()) {
			case Event.NODE_ADDED : type = "NODE_ADDED";
				break;
			case Event.NODE_MOVED : type = "NODE_MOVED";
				break;
			case Event.NODE_REMOVED : type = "NODE_REMOVED";
				break;
			case Event.PROPERTY_ADDED : type = "PROPERTY_ADDED";
				break;
			case Event.PROPERTY_CHANGED : type = "PROPERTY_CHANGED";
				break;
			case Event.PROPERTY_REMOVED : type = "PROPERTY_REMOVED";
				break;
		}
		return type;
	}

}
