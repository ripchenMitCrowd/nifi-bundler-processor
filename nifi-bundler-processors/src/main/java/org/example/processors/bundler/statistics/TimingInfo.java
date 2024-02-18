package org.example.processors.bundler.statistics;

import lombok.Getter;
import org.apache.nifi.logging.ComponentLog;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Provides a mechanism for timing how long a particular operation takes to complete, logging if it takes longer than the configured threshold.
 */
@Getter
public class TimingInfo {
    private final String directory;
    private final String filename;
    private final int[] operationTimes;
    private final PerformanceTracker tracker;
    private final long creationTimestamp;
    private final ComponentLog logger;

    public TimingInfo(final String directory, final String filename, final PerformanceTracker tracker, final ComponentLog logger) {
        this.directory = directory;
        this.filename = filename;
        this.tracker = tracker;
        this.logger = logger;

        this.creationTimestamp = System.currentTimeMillis();

        operationTimes = new int[DiskOperation.values().length];
        Arrays.fill(operationTimes, -1);
    }

    public void accept(final DiskOperation operation, final long duration) {
        operationTimes[operation.ordinal()] = (int) duration;

        if (logger.isTraceEnabled()) {
            logger.trace("Performing operation {} on {} took {} milliseconds", operation, getFullPath(), duration);
        }
    }

    private String getFullPath() {
        if (directory.isEmpty()) {
            return filename;
        } else {
            return directory.endsWith("/") ? directory + filename : directory + "/" + filename;
        }
    }

    public long getOperationTime(final DiskOperation operation) {
        return operationTimes[operation.ordinal()];
    }

    public <T> T timeOperation(final DiskOperation operation, final Supplier<T> function) {
        final long start = System.currentTimeMillis();
        final TimedOperationKey operationKey = new TimedOperationKey(operation, directory, filename, start);
        tracker.setActiveOperation(operationKey);

        try {
            final T value = function.get();
            final long millis = System.currentTimeMillis() - start;
            accept(operation, millis);
            return value;
        } finally {
            tracker.completeActiveOperation();
        }
    }

    public void timeOperation(final DiskOperation operation, final Runnable task) {
        final long start = System.currentTimeMillis();
        final TimedOperationKey operationKey = new TimedOperationKey(operation, directory, filename, start);
        tracker.setActiveOperation(operationKey);

        try {
            task.run();
            final long millis = System.currentTimeMillis() - start;
            accept(operation, millis);
        } finally {
            tracker.completeActiveOperation();
        }
    }
}
