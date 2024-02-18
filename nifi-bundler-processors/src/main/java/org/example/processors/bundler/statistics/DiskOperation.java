package org.example.processors.bundler.statistics;

public enum DiskOperation {
    RETRIEVE_BASIC_ATTRIBUTES,
    RETRIEVE_OWNER_ATTRIBUTES,
    RETRIEVE_POSIX_ATTRIBUTES,
    CHECK_HIDDEN,
    CHECK_READABLE,
    FILTER,
    RETRIEVE_NEXT_FILE_FROM_OS
}
