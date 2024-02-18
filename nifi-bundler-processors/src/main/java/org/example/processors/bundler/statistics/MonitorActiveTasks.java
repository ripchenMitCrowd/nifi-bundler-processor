package org.example.processors.bundler.statistics;

import org.apache.nifi.logging.ComponentLog;

import java.util.concurrent.TimeUnit;

public class MonitorActiveTasks implements Runnable {
    private final PerformanceTracker performanceTracker;
    private final ComponentLog logger;
    private final long maxListingMillis;
    private final long millisToKeepStats;
    private long lastPurgeTimestamp = 0L;

    public MonitorActiveTasks(final PerformanceTracker tracker, final ComponentLog logger, final long maxListingMillis, final long millisToKeepStats) {
        this.performanceTracker = tracker;
        this.logger = logger;
        this.maxListingMillis = maxListingMillis;
        this.millisToKeepStats = millisToKeepStats;
    }

    @Override
    public void run() {
        monitorActiveDirectory();

        final long now = System.currentTimeMillis();
        final long millisSincePurge = now - lastPurgeTimestamp;
        if (millisSincePurge > TimeUnit.SECONDS.toMillis(60)) {
            performanceTracker.purgeTimingInfo(now - millisToKeepStats);
            lastPurgeTimestamp = System.currentTimeMillis();
        }
    }

    private void monitorActiveDirectory() {
        final String activeDirectory = performanceTracker.getActiveDirectory();
        final long startTime = performanceTracker.getActiveDirectoryStartTime();
        if (startTime <= 0) {
            return;
        }

        final long activeMillis = System.currentTimeMillis() - startTime;
        if (activeMillis > maxListingMillis) {
            final String fullPath = activeDirectory.isEmpty() ? "the base directory" : activeDirectory;
            logger.warn("This processor has currently spent {} milliseconds performing the listing of {}, which exceeds the configured threshold of {} milliseconds",
                    activeMillis, fullPath, maxListingMillis);
        }
    }
}
