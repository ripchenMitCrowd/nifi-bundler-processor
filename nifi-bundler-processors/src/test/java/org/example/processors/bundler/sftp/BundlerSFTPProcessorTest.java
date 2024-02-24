package org.example.processors.bundler.sftp;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.example.processors.bundler.sftp.BundlerSFTPProcessor.*;


class BundlerSFTPProcessorTest {

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new BundlerSFTPProcessor() {
            @Override
            protected FileTransfer createFileTransfer(ProcessContext context) {
                return new BundlingSFTPTransfer(context, getLogger()) {
                    @Override
                    public List<FileInfo> getListing(boolean applyFilters) {
                        return List.of(
                                new FileInfo.Builder().fullPathFileName("C:\\path\\test.txt").filename("test.txt").build(),
                                new FileInfo.Builder().fullPathFileName("C:\\path\\test.001").filename("test.001").build()
                        );
                    }

                    @Override
                    protected void downloadFile(String remoteFileName, FlowFile origFlowFile, String partName, ZipOutputStream zos) throws IOException {
                        zos.write("test".getBytes());
                    }

                    @Override
                    public void deleteFile(FlowFile flowFile, String path, String remoteFileName) {
                        // do nothing
                    }
                };
            }
        });
    }

    @Test
    void onTrigger() throws IOException {
        runner.setProperty(HOSTNAME, "localhost");
        runner.setProperty(USERNAME, "user");
        runner.setProperty(REMOTE_FILENAME, "C:\\path\\test.txt");
        runner.setProperty(REMOTE_PATH, "C:\\path\\");
        runner.enqueue(new byte[0]);
        runner.run();

        var results = runner.getFlowFilesForRelationship(BundlerSFTPProcessor.REL_SUCCESS);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst().getData()).hasSize(250);
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(results.getFirst().getData()))) {
            ZipEntry entry;
            while((entry = zis.getNextEntry())!=null) {
                String fileContent = IOUtils.toString(zis, StandardCharsets.UTF_8);
                assertThat(entry.getName()).startsWith("test");
                assertThat(entry.getSize()).isEqualTo(4);
                assertThat(fileContent).isEqualTo("test");
            }
        }
    }

    @Test
    void cleanup() {
        runner.stop();
        new BundlerSFTPProcessor().cleanup();
    }
}