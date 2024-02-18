package org.example.processors.bundler.statistics;

import java.util.Collections;
import java.util.Map;

public interface OperationStatistics {

    long min();
    long max();
    long count();
    double average();
    double getStandardDeviation();

    Map<String, Long> outliers();

    OperationStatistics EMPTY = new OperationStatistics() {
        @Override
        public long min() {
            return 0;
        }

        @Override
        public long max() {
            return 0;
        }

        @Override
        public long count() {
            return 0;
        }

        @Override
        public double average() {
            return 0;
        }

        @Override
        public double getStandardDeviation() {
            return 0;
        }

        @Override
        public Map<String, Long> outliers() {
            return Collections.emptyMap();
        }
    };
}
