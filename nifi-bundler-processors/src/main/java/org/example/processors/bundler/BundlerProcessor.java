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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "fetch"})
@CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile. Once this is done, the file is optionally moved elsewhere or deleted to help keep the file system organized.")
@Restricted(
        restrictions = {@Restriction(
                requiredPermission = RequiredPermission.READ_FILESYSTEM,
                explanation = "Provides operator the ability to read from any file that NiFi has access to."
        ), @Restriction(
                requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                explanation = "Provides operator the ability to delete any file that NiFi has access to."
        )}
)
public class BundlerProcessor extends AbstractProcessor {

    static final PropertyDescriptor FILENAME = (new PropertyDescriptor.Builder()).name("File to Fetch").description("The fully-qualified filename of the file to fetch from the file system").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).defaultValue("${absolute.path}/${filename}").required(true).build();
    static final Relationship REL_SUCCESS  = (new Relationship.Builder()).name("success").description("Any FlowFile that is successfully fetched from the file system will be transferred to this Relationship.").build();
    static final Relationship REL_NOT_FOUND = (new Relationship.Builder()).name("not.found").description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.").build();
    static final Relationship REL_PERMISSION_DENIED = (new Relationship.Builder()).name("permission.denied").description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.").build();
    static final Relationship REL_FAILURE = (new Relationship.Builder()).name("failure").description("Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.").build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FILENAME);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_PERMISSION_DENIED);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return new ArrayList<>();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        StopWatch stopWatch = new StopWatch(true);
        String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        File file = new File(filename);
        Path filePath = file.toPath();

        if (!Files.exists(filePath) && !Files.notExists(filePath)) {
            this.getLogger().log(LogLevel.ERROR, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure", file, flowFile);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        } else if (!Files.exists(filePath)) {
            this.getLogger().log(LogLevel.ERROR, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", file, flowFile);
            session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
            session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
        } else {
            if (!file.canRead()) {
                this.getLogger().log(LogLevel.ERROR, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied", file, flowFile, System.getProperty("user.name"));
                session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
                session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
            } else {
                final String baseName = FilenameUtils.getBaseName(file.getName());
                final File parentDir = file.getParentFile();

                final File[] bundleFiles = parentDir.listFiles((dir, name) -> name.contains(baseName));
                if (bundleFiles == null) {
                    this.getLogger().error("No files found to bundle with baseName {} in {}", baseName, parentDir);
                    return;
                }
                final Path bundledZip = Path.of(parentDir.toURI()).resolve(baseName+".zip");

                try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(bundledZip.toFile())))) {
                    for (File part : bundleFiles) {
                        ZipEntry zipEntry = new ZipEntry(part.getName());
                        zipEntry.setSize(bundledZip.toFile().length());
                        zos.putNextEntry(zipEntry);
                        try (FileInputStream fis = new FileInputStream(part);
                             InputStream in = new BufferedInputStream(fis)) {
                            IOUtils.copy(in,zos);
                        }
                        zos.closeEntry();
                    }
                } catch (IOException e) {
                    this.getLogger().error("Could not fetch file {} from file system for {} due to {}; routing to failure", file, flowFile, e.toString(), e);
                }

                try (FileInputStream fis = new FileInputStream(bundledZip.toFile())) {
                    flowFile = session.importFrom(fis, flowFile);
                } catch (IOException e) {
                    this.getLogger().error("Could not fetch file {} from file system for {} due to {}; routing to failure", file, flowFile, e.toString(), e);
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                    return;
                }
                FlowFile bundledFlowFile = flowFile;
                bundledFlowFile = session.putAllAttributes(bundledFlowFile, Map.of(FILENAME.getName(),bundledZip.toString()));

                session.getProvenanceReporter().fetch(flowFile, file.toURI().toString(), "Replaced content of FlowFile with contents of " + file.toURI(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(bundledFlowFile, REL_SUCCESS);
                session.commitAsync(() -> this.performCompletionAction(bundleFiles));
            }
        }
    }

    private void performCompletionAction(File[] bundleFiles) {
        for (File part : bundleFiles) {
            try {
               Files.delete(part.toPath());
            } catch (IOException e) {
                this.getLogger().warn("Failed to delete processed file {} due to {}; routing to success", part.getAbsolutePath(), e);
            }
        }
    }
}
