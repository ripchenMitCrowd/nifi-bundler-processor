package org.example.processors.bundler.statistics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.nifi.logging.ComponentLog;

/**
 * A PerformanceTracker that is capable of tracking which disk access operation is active and which directory is actively being listed,
 * as well as timing specific operations, but does not track metrics over any amount of time. This implementation does not provide the ability
 * to glean information such as which operations or files are taking the longest to operate on but uses very little heap.
 */
@Getter
@RequiredArgsConstructor
public class UntrackedPerformanceTracker implements PerformanceTracker {
    @Setter
    private TimedOperationKey activeOperation = null;
    private String activeDirectory;
    private long activeDirectoryStartTime = -1L;

    private final ComponentLog logger;

    @Override
    public TimedOperationKey beginOperation(final DiskOperation operation, final String directory, final String filename) {
        return null;
    }

    @Override
    public void completeOperation(final TimedOperationKey operationKey) {}

    @Override
    public void acceptOperation(final DiskOperation operation, final String directory, final String filename, final long millis) {}

    @Override
    public TimingInfo getTimingInfo(final String directory, final String filename) {
        return new TimingInfo(directory, filename, this, logger);
    }

    @Override
    public OperationStatistics getOperationStatistics(final DiskOperation operation) {
        return OperationStatistics.EMPTY;
    }

    @Override
    public synchronized void completeActiveOperation() {
        this.activeOperation = null;
    }

    @Override
    public void purgeTimingInfo(final long cutoff) {}

    @Override
    public long getEarliestTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public synchronized void setActiveDirectory(final String directory) {
        activeDirectory = directory;
        activeDirectoryStartTime = System.currentTimeMillis();
    }

    @Override
    public synchronized void completeActiveDirectory() {
        activeDirectory = null;
        activeDirectoryStartTime = -1L;
    }
}
