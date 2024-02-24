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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.PermissionDeniedException;
import org.apache.nifi.processors.standard.*;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sftp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote SFTP server and overwrites the contents of an incoming FlowFile with the content of the remote file.")
@SeeAlso({ListSFTP.class})
@WritesAttributes({@WritesAttribute(
        attribute = "sftp.remote.host",
        description = "The hostname or IP address from which the file was pulled"
), @WritesAttribute(
        attribute = "sftp.remote.port",
        description = "The port that was used to communicate with the remote SFTP server"
), @WritesAttribute(
        attribute = "sftp.remote.filename",
        description = "The name of the remote file that was pulled"
), @WritesAttribute(
        attribute = "filename",
        description = "The filename is updated to point to the filename fo the remote file"
), @WritesAttribute(
        attribute = "path",
        description = "If the Remote File contains a directory name, that directory name will be added to the FlowFile using the 'path' attribute"
), @WritesAttribute(
        attribute = "fetch.failure.reason",
        description = "The name of the failure relationship applied when routing to any failure relationship"
)})
@MultiProcessorUseCase(
        description = "Retrieve all files in a directory of an SFTP Server",
        keywords = {"sftp", "secure", "file", "transform", "state", "retrieve", "fetch", "all", "stream"},
        configurations = {@ProcessorConfiguration(
                processorClass = ListSFTP.class,
                configuration = "The \"Hostname\" property should be set to the fully qualified hostname of the FTP Server. It's a good idea to parameterize     this property by setting it to something like `#{SFTP_SERVER}`.\nThe \"Remote Path\" property must be set to the directory on the FTP Server where the files reside. If the flow being built is to be reused elsewhere,     it's a good idea to parameterize this property by setting it to something like `#{SFTP_REMOTE_PATH}`.\nConfigure the \"Username\" property to the appropriate username for logging into the FTP Server. It's usually a good idea to parameterize this property     by setting it to something like `#{SFTP_USERNAME}`.\nConfigure the \"Password\" property to the appropriate password for the provided username. It's usually a good idea to parameterize this property     by setting it to something like `#{SFTP_PASSWORD}`.\n\nThe 'success' Relationship of this Processor is then connected to FetchSFTP.\n"
        ), @ProcessorConfiguration(
                processorClass = BundlerSFTPProcessor.class,
                configuration = "\"Hostname\" = \"${sftp.remote.host}\"\n\"Remote File\" = \"${path}/${filename}\"\n\"Username\" = \"${sftp.listing.user}\"\n\"Password\" = \"#{SFTP_PASSWORD}\"\n"
        )}
)
public class BundlerSFTPProcessor extends AbstractProcessor {

    public static final String FAILURE_REASON_ATTRIBUTE = "fetch.failure.reason";

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The fully-qualified hostname or IP address of the host to fetch the data from")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();
    public static final PropertyDescriptor UNDEFAULTED_PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("The port to connect to on the remote host to fetch the data from")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();
    public static final PropertyDescriptor REMOTE_FILENAME = new PropertyDescriptor.Builder()
            .name("Remote File")
            .description("The fully qualified filename on the remote system")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();
    public static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
            .name("comms.failure")
            .description("Any FlowFile that could not be fetched from the remote server due to a communications failure will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("Any FlowFile for which we receive a 'Not Found' message from the remote server will be transferred to this Relationship.")
            .build();
    public static final Relationship REL_PERMISSION_DENIED = new Relationship.Builder()
            .name("permission.denied")
            .description("Any FlowFile that could not be fetched from the remote server due to insufficient permissions will be transferred to this Relationship.")
            .build();

    private final Map<Tuple<String, Integer>, BlockingQueue<FileTransferIdleWrapper>> fileTransferMap = new HashMap<>();
    private final long IDLE_CONNECTION_MILLIS = TimeUnit.SECONDS.toMillis(10L); // amount of time to wait before closing an idle connection
    private volatile long lastClearTime = System.currentTimeMillis();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor port = new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("22").build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(port);
        properties.add(USERNAME);
        properties.add(SFTPTransfer.PASSWORD);
        properties.add(SFTPTransfer.PRIVATE_KEY_PATH);
        properties.add(SFTPTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(REMOTE_FILENAME);
        properties.add(SFTPTransfer.CONNECTION_TIMEOUT);
        properties.add(SFTPTransfer.DATA_TIMEOUT);
        properties.add(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(SFTPTransfer.HOST_KEY_FILE);
        properties.add(SFTPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SFTPTransfer.USE_COMPRESSION);
        properties.add(SFTPTransfer.PROXY_CONFIGURATION_SERVICE);
        properties.add(FTPTransfer.PROXY_TYPE);
        properties.add(FTPTransfer.PROXY_HOST);
        properties.add(FTPTransfer.PROXY_PORT);
        properties.add(FTPTransfer.HTTP_PROXY_USERNAME);
        properties.add(FTPTransfer.HTTP_PROXY_PASSWORD);
        properties.add(SFTPTransfer.CIPHERS_ALLOWED);
        properties.add(SFTPTransfer.KEY_ALGORITHMS_ALLOWED);
        properties.add(SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED);
        properties.add(SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_PERMISSION_DENIED);
        relationships.add(REL_COMMS_FAILURE);
        return relationships;
    }

    /**
     * Close connections that are idle or optionally close all connections.
     * Connections are considered "idle" if they have not been used in 10 seconds.
     *
     * @param closeNonIdleConnections if <code>true</code> will close all connection; if <code>false</code> will close only idle connections
     */
    private void closeConnections(final boolean closeNonIdleConnections) {
        for (final Map.Entry<Tuple<String, Integer>, BlockingQueue<FileTransferIdleWrapper>> entry : fileTransferMap.entrySet()) {
            final BlockingQueue<FileTransferIdleWrapper> wrapperQueue = entry.getValue();

            final List<FileTransferIdleWrapper> putBack = new ArrayList<>();
            FileTransferIdleWrapper wrapper;
            while ((wrapper = wrapperQueue.poll()) != null) {
                final long lastUsed = wrapper.lastUsed();
                final long nanosSinceLastUse = System.nanoTime() - lastUsed;
                if (!closeNonIdleConnections && TimeUnit.NANOSECONDS.toMillis(nanosSinceLastUse) < IDLE_CONNECTION_MILLIS) {
                    putBack.add(wrapper);
                } else {
                    try {
                        wrapper.fileTransfer().close();
                    } catch (final IOException ioe) {
                        getLogger().warn("Failed to close Idle Connection due to {}", ioe, ioe);
                    }
                }
            }

            for (final FileTransferIdleWrapper toPutBack : putBack) {
                wrapperQueue.offer(toPutBack);
            }
        }
    }

    @OnStopped
    public void cleanup() {
        closeConnections(true);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final int port = context.getProperty(UNDEFAULTED_PORT).evaluateAttributeExpressions(flowFile).asInteger();
        final String filename = context.getProperty(REMOTE_FILENAME).evaluateAttributeExpressions(flowFile).getValue();

        // Try to get a FileTransfer object from our cache.
        BlockingQueue<FileTransferIdleWrapper> transferQueue;
        synchronized (fileTransferMap) {
            final Tuple<String, Integer> tuple = new Tuple<>(host, port);

            transferQueue = fileTransferMap.get(tuple);
            if (transferQueue == null) {
                transferQueue = new LinkedBlockingQueue<>();
                fileTransferMap.put(tuple, transferQueue);
            }

            // periodically close idle connections
            if (System.currentTimeMillis() - lastClearTime > IDLE_CONNECTION_MILLIS) {
                closeConnections(false);
                lastClearTime = System.currentTimeMillis();
            }
        }

        // we have a queue of FileTransfer Objects. Get one from the queue or create a new one.
        FileTransfer transfer;
        FileTransferIdleWrapper transferWrapper = transferQueue.poll();
        if (transferWrapper == null) {
            transfer = createFileTransfer(context);
        } else {
            transfer = transferWrapper.fileTransfer();
        }

        Relationship failureRelationship = null;
        boolean closeConnOnFailure = false;

        try {
            // Pull data from remote system.
            try {
                flowFile = transfer.getRemoteFile(filename, flowFile, session);
            } catch (final FileNotFoundException e) {
                failureRelationship = REL_NOT_FOUND;
                getLogger().log(LogLevel.ERROR, "Failed to fetch content for {} from filename {} on remote host {} because the file could not be found on the remote system; routing to {}",
                        flowFile, filename, host, failureRelationship.getName());
            } catch (final PermissionDeniedException e) {
                failureRelationship = REL_PERMISSION_DENIED;
                getLogger().error("Failed to fetch content for {} from filename {} on remote host {} due to insufficient permissions; routing to {}",
                        flowFile, filename, host, failureRelationship.getName());
            } catch (final ProcessException | IOException e) {
                failureRelationship = REL_COMMS_FAILURE;
                getLogger().error("Failed to fetch content for {} from filename {} on remote host {}:{} due to {}; routing to {}",
                        flowFile, filename, host, port, e.toString(), failureRelationship.getName(), e);

                closeConnOnFailure = true;
            }

            // Add FlowFile attributes
            final Map<String, String> attributes = new HashMap<>();
            final String protocolName = transfer.getProtocolName();

            attributes.put(protocolName + ".remote.host", host);
            attributes.put(protocolName + ".remote.port", String.valueOf(port));
            attributes.put(protocolName + ".remote.filename", filename);

            if (filename.contains("/")) {
                final String path = StringUtils.substringBeforeLast(filename, "/");
                final String filenameOnly = StringUtils.substringAfterLast(filename, "/");
                attributes.put(CoreAttributes.PATH.key(), path);
                attributes.put(CoreAttributes.FILENAME.key(), filenameOnly);
            } else {
                attributes.put(CoreAttributes.FILENAME.key(), filename);
            }

            if (failureRelationship != null) {
                attributes.put(FAILURE_REASON_ATTRIBUTE, failureRelationship.getName());
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(session.penalize(flowFile), failureRelationship);
                session.getProvenanceReporter().route(flowFile, failureRelationship);
                cleanupTransfer(transfer, closeConnOnFailure, transferQueue, host, port);
                return;
            }

            flowFile = session.putAllAttributes(flowFile, attributes);

            // emit provenance event and transfer FlowFile
            session.getProvenanceReporter().fetch(flowFile, protocolName + "://" + host + ":" + port + "/" + filename, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);

            // TODO download all parts & bundle into zip

            // it is critical that we commit the session before moving/deleting the remote file. Otherwise, we could have a situation where
            // we ingest the data, delete/move the remote file, and then NiFi dies/is shut down before the session is committed. This would
            // result in data loss! If we commit the session first, we are safe.
            final BlockingQueue<FileTransferIdleWrapper> queue = transferQueue;
            final Runnable cleanupTask = () -> cleanupTransfer(transfer, false, queue, host, port);

            final FlowFile flowFileReceived = flowFile;
            session.commitAsync(() -> {
                performCompletionStrategy(transfer, context, flowFileReceived, filename, host, port);
                cleanupTask.run();
            }, t -> cleanupTask.run());
        } catch (final Throwable t) {
            getLogger().error("Failed to fetch file", t);
            cleanupTransfer(transfer, true, transferQueue, host, port);
        }
    }

    private void cleanupTransfer(final FileTransfer transfer, final boolean closeConnection, final BlockingQueue<FileTransferIdleWrapper> transferQueue, final String host, final int port) {
        if (closeConnection) {
            getLogger().debug("Closing FileTransfer...");
            try {
                transfer.close();
            } catch (final IOException e) {
                getLogger().warn("Failed to close connection to {}:{} due to {}", host, port, e.getMessage(), e);
            }
        } else {
            getLogger().debug("Returning FileTransfer to pool...");
            transferQueue.offer(new FileTransferIdleWrapper(transfer, System.nanoTime()));
        }
    }

    private void performCompletionStrategy(final FileTransfer transfer, final ProcessContext context, final FlowFile flowFile, final String filename, final String host, final int port) {
        try {
            transfer.deleteFile(flowFile, null, filename);
        } catch (final FileNotFoundException e) {
            // file doesn't exist -- effectively the same as removing it. Move on.
        } catch (final IOException ioe) {
            getLogger().warn("Successfully fetched the content for {} from {}:{}{} but failed to remove the remote file due to {}",
                    flowFile, host, port, filename, ioe, ioe);
        }
    }


    /**
     * Creates a new instance of a FileTransfer that can be used to pull files from a remote system.
     *
     * @param context the ProcessContext to use in order to obtain configured properties
     * @return a FileTransfer that can be used to pull files from a remote system
     */
    protected FileTransfer createFileTransfer(ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    /**
     * Wrapper around a FileTransfer object that is used to know when the FileTransfer was last used, so that
     * we have the ability to close connections that are "idle," or unused for some period of time.
     */
    private record FileTransferIdleWrapper(FileTransfer fileTransfer, long lastUsed) {}
}
