package org.example.processors.bundler.statistics;

/**
 * PerformanceTracker is responsible for providing a mechanism by which any disk operation can be timed and the timing information
 * can both be used to issue warnings as well as be aggregated for some amount of time, in order to understand how long certain disk operations
 * take and which files may be responsible for causing longer-than-usual operations to be performed.
 */
public interface PerformanceTracker {
    TimedOperationKey beginOperation(DiskOperation operation, String directory, String filename);

    void completeOperation(TimedOperationKey operationKey);

    void acceptOperation(DiskOperation operation, String directory, String filename, long millis);

    TimingInfo getTimingInfo(String directory, String filename);

    OperationStatistics getOperationStatistics(DiskOperation operation);

    void setActiveOperation(TimedOperationKey operationKey);

    void completeActiveOperation();

    void purgeTimingInfo(long cutoff);

    long getEarliestTimestamp();

    void setActiveDirectory(String directory);

    void completeActiveDirectory();

    String getActiveDirectory();

    long getActiveDirectoryStartTime();
}
