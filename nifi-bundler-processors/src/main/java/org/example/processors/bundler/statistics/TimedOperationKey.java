package org.example.processors.bundler.statistics;

public record TimedOperationKey(DiskOperation operation, String directory, String filename, long startTime) {
}
