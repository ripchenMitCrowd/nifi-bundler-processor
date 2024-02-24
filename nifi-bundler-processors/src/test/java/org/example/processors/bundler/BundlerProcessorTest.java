/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.processors.bundler;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.example.processors.bundler.BundlerProcessor.FILENAME;
import static org.example.processors.bundler.BundlerProcessor.REL_SUCCESS;

public class BundlerProcessorTest {

    private final String TESTDIR = "target/test/data/in";
    private final File testDir = new File(TESTDIR);
    private final byte[] content = "Hello, World!".getBytes();

    @BeforeAll
    public static void setupClass() throws Exception {
        Files.createTempFile(Paths.get("target/"), "TestBundlerProcessor", null).toFile();
    }

    @BeforeEach
    public void setUp() {
        deleteDirectory(testDir);

        assertThat(testDir.exists() || testDir.mkdirs())
                .withFailMessage("Unable to create test data directory " + testDir.getAbsolutePath())
                .isTrue();
    }

    @AfterEach
    public void tearDown()  {
        deleteDirectory(testDir);
    }

    @Test
    void performListing() throws IOException {
        Path file1 = Path.of(TESTDIR + "/listing1.xml");
        Files.write(file1, content, StandardOpenOption.CREATE);
        Path file11 =  Path.of(TESTDIR + "/listing1.001");
        Files.write(file11, content, StandardOpenOption.CREATE);
        Path file2 = Path.of(TESTDIR + "/listing2.xml");
        Files.write(file2, content, StandardOpenOption.CREATE);
        Path file21 =  Path.of(TESTDIR + "/listing2.001");
        Files.write(file21, content, StandardOpenOption.CREATE);
        Path file22 =  Path.of(TESTDIR + "/listing2.002");
        Files.write(file22, content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new BundlerProcessor());

        runner.setProperty(FILENAME, file1.toFile().getAbsolutePath());
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.setProperty(FILENAME, file2.toFile().getAbsolutePath());
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        var results = runner.getFlowFilesForRelationship(REL_SUCCESS);
        assertThat(results).hasSize(2);
        assertThat(results.get(1).getAttribute(FILENAME.getName())).isEqualTo("C:\\Users\\marti\\Documents\\InteliJProjects\\bundler\\nifi-bundler-processors\\target\\test\\data\\in\\listing2.zip");
        assertThat(results.get(0).getAttribute(FILENAME.getName())).isEqualTo("C:\\Users\\marti\\Documents\\InteliJProjects\\bundler\\nifi-bundler-processors\\target\\test\\data\\in\\listing1.zip");

        Path bundledFile1 = Path.of(TESTDIR + "/listing1.zip");
        assertThat(bundledFile1.toFile()).exists();
        Path bundledFile2 = Path.of(TESTDIR + "/listing2.zip");
        assertThat(bundledFile2.toFile()).exists();

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(results.getFirst().getAttribute(FILENAME.getName())))) {
            ZipEntry entry;
            while((entry = zis.getNextEntry())!=null) {
                String fileContent = IOUtils.toString(zis, StandardCharsets.UTF_8);
                assertThat(entry.getName()).startsWith("listing");
                assertThat(entry.getSize()).isEqualTo(13);
                assertThat(fileContent).isEqualTo(new String(content));
            }
        }

        assertThat(file1.toFile()).doesNotExist();
        assertThat(file11.toFile()).doesNotExist();
        assertThat(file2.toFile()).doesNotExist();
        assertThat(file21.toFile()).doesNotExist();
        assertThat(file22.toFile()).doesNotExist();
    }

    private void deleteDirectory(final File directory) {
        if (!directory.exists()) {
            return;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }
        for (final File file : files) {
            if (file.isDirectory()) {
                deleteDirectory(file);
            } else {
                assertThat(file.delete()).isTrue();
            }
        }
    }

}
