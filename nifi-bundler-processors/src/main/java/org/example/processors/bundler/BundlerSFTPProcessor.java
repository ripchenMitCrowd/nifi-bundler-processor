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
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
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
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Proxy;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sftp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote SFTP server and overwrites the contents of an incoming FlowFile with the content of the remote file.")
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
public class BundlerSFTPProcessor extends AbstractProcessor {

    private static final SSHClientProvider SSH_CLIENT_PROVIDER = new StandardSSHClientProvider();

    private static final String DOT_PREFIX = ".";
    private static final String RELATIVE_CURRENT_DIRECTORY = DOT_PREFIX;
    private static final String RELATIVE_PARENT_DIRECTORY = "..";

    private static final Set<String> DEFAULT_KEY_ALGORITHM_NAMES;
    private static final Set<String> DEFAULT_CIPHER_NAMES;
    private static final Set<String> DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES;
    private static final Set<String> DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES;

    static {
        DefaultConfig defaultConfig = new DefaultConfig();

        DEFAULT_KEY_ALGORITHM_NAMES = Collections.unmodifiableSet(defaultConfig.getKeyAlgorithms().stream()
                .map(Factory.Named::getName).collect(Collectors.toSet()));
        DEFAULT_CIPHER_NAMES = Collections.unmodifiableSet(defaultConfig.getCipherFactories().stream()
                .map(Factory.Named::getName).collect(Collectors.toSet()));
        DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES = Collections.unmodifiableSet(defaultConfig.getMACFactories().stream()
                .map(Factory.Named::getName).collect(Collectors.toSet()));
        DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES = Collections.unmodifiableSet(defaultConfig.getKeyExchangeFactories().stream()
                .map(Factory.Named::getName).collect(Collectors.toSet()));
    }

    /**
     * Converts a set of names into an alphabetically ordered comma separated value list.
     *
     * @param factorySetNames The set of names
     * @return An alphabetically ordered comma separated value list of names
     */
    private static String convertFactorySetToString(Set<String> factorySetNames) {
        return factorySetNames
                .stream()
                .sorted()
                .collect(Collectors.joining(", "));
    }

    public static final String FAILURE_REASON_ATTRIBUTE = "fetch.failure.reason";

    public static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
            .name("Private Key Path")
            .description("The fully qualified path to the Private Key file")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor PRIVATE_KEY_PASSPHRASE = new PropertyDescriptor.Builder()
            .name("Private Key Passphrase")
            .description("Password for the private key")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor HOST_KEY_FILE = new PropertyDescriptor.Builder()
            .name("Host Key File")
            .description("If supplied, the given file will be used as the Host Key;" +
                    " otherwise, if 'Strict Host Key Checking' property is applied (set to true)" +
                    " then uses the 'known_hosts' and 'known_hosts2' files from ~/.ssh directory" +
                    " else no host key file will be used")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .required(false)
            .build();
    public static final PropertyDescriptor STRICT_HOST_KEY_CHECKING = new PropertyDescriptor.Builder()
            .name("Strict Host Key Checking")
            .description("Indicates whether or not strict enforcement of hosts keys should be applied")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("The port that the remote system is listening on for file transfers")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("22")
            .build();
    public static final PropertyDescriptor USE_KEEPALIVE_ON_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Send Keep Alive On Timeout")
            .description("Send a Keep Alive message every 5 seconds up to 5 times for an overall timeout of 25 seconds.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor KEY_ALGORITHMS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Key Algorithms Allowed")
            .displayName("Key Algorithms Allowed")
            .description("A comma-separated list of Key Algorithms allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_KEY_ALGORITHM_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CIPHERS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Ciphers Allowed")
            .displayName("Ciphers Allowed")
            .description("A comma-separated list of Ciphers allowed for SFTP connections. Leave unset to allow all. Available options are: " + convertFactorySetToString(DEFAULT_CIPHER_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MESSAGE_AUTHENTICATION_CODES_ALLOWED = new PropertyDescriptor.Builder()
            .name("Message Authentication Codes Allowed")
            .displayName("Message Authentication Codes Allowed")
            .description("A comma-separated list of Message Authentication Codes allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_MESSAGE_AUTHENTICATION_CODE_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_EXCHANGE_ALGORITHMS_ALLOWED = new PropertyDescriptor.Builder()
            .name("Key Exchange Algorithms Allowed")
            .displayName("Key Exchange Algorithms Allowed")
            .description("A comma-separated list of Key Exchange Algorithms allowed for SFTP connections. Leave unset to allow all. Available options are: "
                    + convertFactorySetToString(DEFAULT_KEY_EXCHANGE_ALGORITHM_NAMES))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
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

    public static final String CONNECTION_MODE_ACTIVE = "Active";
    public static final String CONNECTION_MODE_PASSIVE = "Passive";
    public static final String TRANSFER_MODE_ASCII = "ASCII";
    public static final String TRANSFER_MODE_BINARY = "Binary";
    public static final String FTP_TIMEVAL_FORMAT = "yyyyMMddHHmmss";
    public static final String PROXY_TYPE_DIRECT = Proxy.Type.DIRECT.name();
    public static final String PROXY_TYPE_HTTP = Proxy.Type.HTTP.name();
    public static final String PROXY_TYPE_SOCKS = Proxy.Type.SOCKS.name();

    public static final PropertyDescriptor CONNECTION_MODE = new PropertyDescriptor.Builder()
            .name("Connection Mode")
            .description("The FTP Connection Mode")
            .allowableValues(CONNECTION_MODE_ACTIVE, CONNECTION_MODE_PASSIVE)
            .defaultValue(CONNECTION_MODE_PASSIVE)
            .build();
    public static final PropertyDescriptor TRANSFER_MODE = new PropertyDescriptor.Builder()
            .name("Transfer Mode")
            .description("The FTP Transfer Mode")
            .allowableValues(TRANSFER_MODE_BINARY, TRANSFER_MODE_ASCII)
            .defaultValue(TRANSFER_MODE_BINARY)
            .build();
    public static final PropertyDescriptor PROXY_TYPE = new PropertyDescriptor.Builder()
            .name("Proxy Type")
            .description("Proxy type used for file transfers")
            .allowableValues(PROXY_TYPE_DIRECT, PROXY_TYPE_HTTP, PROXY_TYPE_SOCKS)
            .defaultValue(PROXY_TYPE_DIRECT)
            .build();
    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("The port of the proxy server")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    public static final PropertyDescriptor HTTP_PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("Http Proxy Username")
            .description("Http Proxy Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();
    public static final PropertyDescriptor HTTP_PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("Http Proxy Password")
            .description("Http Proxy Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Internal Buffer Size")
            .description("Set the internal buffer size for buffered data streams")
            .defaultValue("16KB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor UTF8_ENCODING = new PropertyDescriptor.Builder()
            .name("ftp-use-utf8")
            .displayName("Use UTF-8 Encoding")
            .description("Tells the client to use UTF-8 encoding when processing files and filenames. If set to true, the server must also support UTF-8 encoding.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
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
        properties.add(FileTransfer.PASSWORD);
        properties.add(PRIVATE_KEY_PATH);
        properties.add(PRIVATE_KEY_PASSPHRASE);
        properties.add(REMOTE_FILENAME);
        properties.add(FileTransfer.CONNECTION_TIMEOUT);
        properties.add(FileTransfer.DATA_TIMEOUT);
        properties.add(USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(HOST_KEY_FILE);
        properties.add(STRICT_HOST_KEY_CHECKING);
        properties.add(FileTransfer.USE_COMPRESSION);
        properties.add(PROXY_CONFIGURATION_SERVICE);
        properties.add(PROXY_TYPE);
        properties.add(PROXY_HOST);
        properties.add(PROXY_PORT);
        properties.add(HTTP_PROXY_USERNAME);
        properties.add(HTTP_PROXY_PASSWORD);
        properties.add(CIPHERS_ALLOWED);
        properties.add(KEY_ALGORITHMS_ALLOWED);
        properties.add(KEY_EXCHANGE_ALGORITHMS_ALLOWED);
        properties.add(MESSAGE_AUTHENTICATION_CODES_ALLOWED);
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
