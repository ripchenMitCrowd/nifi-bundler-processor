package org.example.processors.bundler;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import static org.example.processors.bundler.BundlerProcessor.FILENAME;

class BundlerSFTPProcessorTest {

    private final TestRunner runner = TestRunners.newTestRunner(new BundlerSFTPProcessor());

    @Test
    void onTrigger() {
        runner.setProperty(FILENAME, "");
        runner.enqueue(new byte[0]);
        runner.run();
    }
}