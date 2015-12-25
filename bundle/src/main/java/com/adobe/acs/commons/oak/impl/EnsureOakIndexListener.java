package com.adobe.acs.commons.oak.impl;

import javax.annotation.Nonnull;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import org.apache.sling.commons.scheduler.ScheduleOptions;
import org.apache.sling.commons.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		executeJob();
	}

	void executeJob() {
		Scheduler scheduler = ensureIndex.getScheduler();
		EnsureOakIndexJobHandler jobHandler =
                new EnsureOakIndexJobHandler(ensureIndex, oakIndexPath, ensureDefinitionsPath);
        ScheduleOptions options = scheduler.NOW();
        String name = String.format("Ensure index %s => %s", new Object[]{ oakIndexPath, ensureDefinitionsPath });
        options.name(name);
        options.canRunConcurrently(false);
        scheduler.schedule(jobHandler, options);

        log.info("Job scheduled for ensuring Oak Indexes [ {} ~> {} ]", ensureDefinitionsPath, oakIndexPath);
	}

}
