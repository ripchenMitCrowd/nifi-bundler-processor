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

import lombok.Getter;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;
import org.example.processors.bundler.statistics.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"file", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from the input directory. For each file listed, creates a FlowFile " +
        "that represents the file so that it can be fetched in conjunction with FetchFile. This Processor is designed " +
        "to run on Primary Node only in a cluster when 'Input Directory Location' is set to 'Remote'. If the primary node " +
        "changes, the new Primary Node will pick up where the previous node left off without duplicating all the data. " +
        "When 'Input Directory Location' is 'Local', the 'Execution' mode can be anything, and synchronization won't happen. " +
        "Unlike GetFile, this Processor does not delete any data from the local filesystem.")
@WritesAttributes({
        @WritesAttribute(attribute="filename", description="The name of the file that was read from filesystem."),
        @WritesAttribute(attribute="path", description="The path is set to the relative path of the file's directory " +
                "on filesystem compared to the Input Directory property. For example, if Input Directory is set to " +
                "/tmp, then files picked up from /tmp will have the path attribute set to \"/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"abc/1/2/3/\"."),
        @WritesAttribute(attribute="absolute.path", description="The absolute.path is set to the absolute path of " +
                "the file's directory on filesystem. For example, if the Input Directory property is set to /tmp, " +
                "then files picked up from /tmp will have the path attribute set to \"/tmp/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"/tmp/abc/1/2/3/\"."),
        @WritesAttribute(attribute=BundlerProcessor.FILE_OWNER_ATTRIBUTE, description="The user that owns the file in filesystem"),
        @WritesAttribute(attribute=BundlerProcessor.FILE_GROUP_ATTRIBUTE, description="The group that owns the file in filesystem"),
        @WritesAttribute(attribute=BundlerProcessor.FILE_SIZE_ATTRIBUTE, description="The number of bytes in the file in filesystem"),
        @WritesAttribute(attribute=BundlerProcessor.FILE_PERMISSIONS_ATTRIBUTE, description="The permissions for the file in filesystem. This " +
                "is formatted as 3 characters for the owner, 3 for the group, and 3 for other users. For example " +
                "rw-rw-r--"),
        @WritesAttribute(attribute=BundlerProcessor.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute=BundlerProcessor.FILE_LAST_ACCESS_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "last accessed as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute=BundlerProcessor.FILE_CREATION_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "created as 'yyyy-MM-dd'T'HH:mm:ssZ'")
})
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run. Whether the state is stored with a Local or Cluster scope depends on the value of the "
        + "<Input Directory Location> property.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class BundlerProcessor extends AbstractListProcessor<FileInfo> {
    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local", "Input Directory is located on a local disk. State will be stored locally on each node in the cluster.");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote", "Input Directory is located on a remote system. State will be stored across the cluster so that "
            + "the listing can be performed on Primary Node Only and another node can pick up where the last node left off, if the Primary Node changes");

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which files to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DIRECTORY_LOCATION = new PropertyDescriptor.Builder()
            .name("Input Directory Location")
            .description("Specifies where the Input Directory is located. This is used to determine whether state should be stored locally or across the cluster.")
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor INCLUDE_FILE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include File Attributes")
            .description("Whether or not to include information such as the file's Last Modified Time and Owner as FlowFile Attributes. "
                    + "Depending on the File System being used, gathering this information can be expensive and as a result should be disabled. This is especially true of remote file shares.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_LISTING_TIME = new PropertyDescriptor.Builder()
            .name("max-listing-time")
            .displayName("Max Directory Listing Time")
            .description("The maximum amount of time that listing any single directory is expected to take. If the listing for the directory specified by the 'Input Directory' property" +
                    "takes longer than this amount of time, a warning bulletin will be generated for each directory listing that exceeds this amount of time.")
            .required(false)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .defaultValue("3 mins")
            .build();

    @Getter
    private List<PropertyDescriptor> supportedPropertyDescriptors;

    @Getter
    private Set<Relationship> relationships;

    private volatile ScheduledExecutorService monitoringThreadPool;
    private volatile Future<?> monitoringFuture;

    private volatile boolean includeFileAttributes;
    private volatile PerformanceTracker performanceTracker;
    private volatile long performanceLoggingTimestamp = System.currentTimeMillis();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.supportedPropertyDescriptors = List.of(DIRECTORY, LISTING_STRATEGY, RECORD_WRITER, DIRECTORY_LOCATION, INCLUDE_FILE_ATTRIBUTES, MIN_AGE, IGNORE_HIDDEN_FILES, TARGET_SYSTEM_TIMESTAMP_PRECISION, ListedEntityTracker.TRACKING_STATE_CACHE, ListedEntityTracker.TRACKING_TIME_WINDOW, ListedEntityTracker.INITIAL_LISTING_TARGET, ListedEntityTracker.NODE_IDENTIFIER, MAX_LISTING_TIME);

        this.relationships = Set.of(REL_SUCCESS);

        monitoringThreadPool = Executors.newScheduledThreadPool(1, r -> {
            final Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("Monitor ListFile Performance [UUID=" + context.getIdentifier() + "]");
            t.setDaemon(true);

            return t;
        });
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        includeFileAttributes = context.getProperty(INCLUDE_FILE_ATTRIBUTES).asBoolean();
        final long maxListingMillis = context.getProperty(MAX_LISTING_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final long millisToKeepStats = TimeUnit.MINUTES.toMillis(15);
        performanceTracker =  new UntrackedPerformanceTracker(getLogger());
        final MonitorActiveTasks monitorTask = new MonitorActiveTasks(performanceTracker, getLogger(), maxListingMillis, millisToKeepStats);
        monitoringFuture = monitoringThreadPool.scheduleAtFixedRate(monitorTask, 15, 15, TimeUnit.SECONDS);
    }

    @OnStopped
    public void onStopped() {
        if (monitoringFuture != null) {
            monitoringFuture.cancel(true);
        }
    }

    public void logPerformance() {
        final ComponentLog logger = getLogger();
        if (!logger.isDebugEnabled()) {
            return;
        }

        final long earliestTimestamp = performanceTracker.getEarliestTimestamp();
        final long millis = System.currentTimeMillis() - earliestTimestamp;
        final long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

        for (final DiskOperation operation : DiskOperation.values()) {
            final OperationStatistics stats = performanceTracker.getOperationStatistics(operation);

            final StringBuilder sb = new StringBuilder();
            if (stats.count() == 0) {
                sb.append("Over the past ").append(seconds).append(" seconds, for Operation '").append(operation).append("' there were no operations performed");
            } else {
                sb.append("Over the past ").append(seconds).append(" seconds, For Operation '").append(operation).append("' there were ")
                        .append(stats.count()).append(" operations performed with an average time of ")
                        .append(stats.average()).append(" milliseconds; Standard Deviation = ").append(stats.getStandardDeviation()).append(" millis; Min Time = ")
                        .append(stats.min()).append(" millis, Max Time = ").append(stats.max()).append(" millis");

                if (logger.isDebugEnabled()) {
                    final Map<String, Long> outliers = stats.outliers();

                    sb.append("; ").append(stats.outliers().size()).append(" significant outliers: ");
                    sb.append(outliers);
                }
            }

            logger.debug(sb.toString());
        }

        performanceLoggingTimestamp = System.currentTimeMillis();
    }


    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();

        final String fullPath = fileInfo.getFullPathFileName();
        final File file = new File(fullPath);
        final Path filePath = file.toPath();
        final Path directoryPath = new File(getPath(context)).toPath();

        final Path relativePath = directoryPath.toAbsolutePath().relativize(filePath.getParent());
        String relativePathString = relativePath.toString();
        relativePathString = relativePathString.isEmpty() ? "." + File.separator : relativePathString + File.separator;

        final Path absPath = filePath.toAbsolutePath();
        final String absPathString = absPath.getParent().toString() + File.separator;

        attributes.put(CoreAttributes.PATH.key(), relativePathString);
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
        attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(fileInfo.getSize()));
        attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatDateTime(fileInfo.getLastModifiedTime()));

        if (includeFileAttributes) {
            final TimingInfo timingInfo = performanceTracker.getTimingInfo(relativePath.toString(), file.getName());

            try {
                FileStore store = Files.getFileStore(filePath);

                timingInfo.timeOperation(DiskOperation.RETRIEVE_BASIC_ATTRIBUTES, () -> {
                    if (store.supportsFileAttributeView("basic")) {
                        try {
                            BasicFileAttributeView view = Files.getFileAttributeView(filePath, BasicFileAttributeView.class);
                            BasicFileAttributes attrs = view.readAttributes();
                            attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatDateTime(attrs.creationTime().toMillis()));
                            attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatDateTime(attrs.lastAccessTime().toMillis()));
                        } catch (Exception ignore) {
                            // allow other attributes if these fail
                        }
                    }
                });

                timingInfo.timeOperation(DiskOperation.RETRIEVE_OWNER_ATTRIBUTES, () -> {
                    if (store.supportsFileAttributeView("owner")) {
                        try {
                            FileOwnerAttributeView view = Files.getFileAttributeView(filePath, FileOwnerAttributeView.class);
                            attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                        } catch (Exception ignore) {
                        } // allow other attributes if these fail
                    }
                });

                timingInfo.timeOperation(DiskOperation.RETRIEVE_POSIX_ATTRIBUTES, () -> {
                    if (store.supportsFileAttributeView("posix")) {
                        try {
                            PosixFileAttributeView view = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
                            attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                            attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                        } catch (Exception ignore) {
                        } // allow other attributes if these fail
                    }
                });
            } catch (IOException ioe) {
                // well then this FlowFile gets none of these attributes
                getLogger().warn("Error collecting attributes for file {}, message is {}", absPathString, ioe.getMessage());
            }
        }

        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        final String location = context.getProperty(DIRECTORY_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            return Scope.CLUSTER;
        }

        return Scope.LOCAL;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return FileInfo.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, 0L, ListingMode.CONFIGURATION_VERIFICATION, false).size();
    }
    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode)
            throws IOException {
        return performListing(context, minTimestamp, listingMode, true);
    }

    private List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode, final boolean applyFilters)
            throws IOException {
        final Path basePath = new File(getPath(context)).toPath();
        final Map<Path, BasicFileAttributes> lastModifiedMap = new HashMap<>();

        final BiPredicate<Path, BasicFileAttributes> fileFilter;
        final PerformanceTracker performanceTracker;
        if (listingMode == ListingMode.EXECUTION) {
            performanceTracker = this.performanceTracker;
        } else {
            performanceTracker = new UntrackedPerformanceTracker(getLogger());
        }
        fileFilter = createFileFilter(context, performanceTracker, applyFilters, basePath);

        final BiPredicate<Path, BasicFileAttributes> matcher = new BiPredicate<>() {
            private long lastTimestamp = System.currentTimeMillis();

            @Override
            public boolean test(final Path path, final BasicFileAttributes attributes) {
                if (!isScheduled() && listingMode == ListingMode.EXECUTION) {
                    throw new ProcessorStoppedException();
                }

                final long now = System.currentTimeMillis();
                final long timeToList = now - lastTimestamp;
                lastTimestamp = now;

                final Path relativeDirectory = basePath.relativize(path).getParent();
                final String relativePath = relativeDirectory == null ? "" : relativeDirectory.toString();
                final String filename = path.getFileName().toString();
                performanceTracker.acceptOperation(DiskOperation.RETRIEVE_NEXT_FILE_FROM_OS, relativePath, filename, timeToList);

                final boolean isDirectory = attributes.isDirectory();
                if (isDirectory) {
                    performanceTracker.setActiveDirectory(relativePath);
                }

                final TimedOperationKey operationKey = performanceTracker.beginOperation(DiskOperation.FILTER, relativePath, filename);

                try {
                    final boolean matchesFilters = (minTimestamp == null || attributes.lastModifiedTime().toMillis() >= minTimestamp)
                            && fileFilter.test(path, attributes);
                    if (!isDirectory && (!applyFilters || matchesFilters)) {
                        // We store the attributes for each Path we are returning in order to avoid
                        // retrieving them again later when creating the FileInfo
                        lastModifiedMap.put(path, attributes);

                        return true;
                    }

                    return false;
                } finally {
                    performanceTracker.completeOperation(operationKey);

                    if (TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - performanceLoggingTimestamp) >= 5) {
                        logPerformance();
                    }
                }
            }
        };

        try {
            final long start = System.currentTimeMillis();
            final List<FileInfo> result = new LinkedList<>();

            Files.walkFileTree(basePath, Collections.singleton(FileVisitOption.FOLLOW_LINKS), 1, new FileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attributes) {
                    if (Files.isReadable(dir)) {
                        return FileVisitResult.CONTINUE;
                    } else {
                        getLogger().debug("The following directory is not readable: {}", dir.toString());
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }

                @Override
                public FileVisitResult visitFile(final Path path, final BasicFileAttributes attributes) {
                    if (matcher.test(path, attributes)) {
                        final File file = path.toFile();
                        final BasicFileAttributes fileAttributes = lastModifiedMap.get(path);
                        final FileInfo fileInfo = new FileInfo.Builder()
                                .directory(false)
                                .filename(file.getName())
                                .fullPathFileName(file.getAbsolutePath())
                                .lastModifiedTime(fileAttributes.lastModifiedTime().toMillis())
                                .size(fileAttributes.size())
                                .build();

                        result.add(fileInfo);
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path path, final IOException e) {
                    if (e instanceof AccessDeniedException) {
                        getLogger().debug("The following file is not readable: {}", path.toString());
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        getLogger().error("Error during visiting file {}: {}", path.toString(), e.getMessage(), e);
                        return FileVisitResult.TERMINATE;
                    }
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException e) {
                    if (e != null) {
                        getLogger().error("Error during visiting directory {}: {}", dir.toString(), e.getMessage(), e);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            final long millis = System.currentTimeMillis() - start;

            getLogger().debug("Took {} milliseconds to perform listing and gather {} entries", millis, result.size());
            return result;
        } catch (final ProcessorStoppedException pse) {
            getLogger().info("Processor was stopped so will not complete listing of Files");
            return Collections.emptyList();
        } finally {
            if (performanceTracker != null) {
                performanceTracker.completeActiveDirectory();
            }
        }
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("%s Directory [%s]", context.getProperty(DIRECTORY_LOCATION).getValue(), getPath(context));
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return DIRECTORY.equals(property) || MIN_AGE.equals(property) || IGNORE_HIDDEN_FILES.equals(property);
    }

    private String formatDateTime(final long dateTime) {
        final ZonedDateTime zonedDateTime = Instant.ofEpochMilli(dateTime).atZone(ZoneId.systemDefault());
        return DATE_TIME_FORMATTER.format(zonedDateTime);
    }

    private BiPredicate<Path, BasicFileAttributes> createFileFilter(final ProcessContext context, final PerformanceTracker performanceTracker,
                                                                    final boolean applyFilters, final Path basePath) {
        final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();

        return (path, attributes) -> {
            if (!applyFilters) {
                return true;
            }

            final long fileAge = System.currentTimeMillis() - attributes.lastModifiedTime().toMillis();
            if (minAge > fileAge) {
                return false;
            }

            final Path relativePath = basePath.relativize(path);
            final Path relativePathParent = relativePath.getParent();
            final String relativeDir = relativePathParent == null ? "" : relativePathParent.toString();
            final String filename = path.getFileName().toString();
            final TimingInfo timingInfo = performanceTracker.getTimingInfo(relativeDir, filename);

            final File file = path.toFile();

            // Verify that we have at least read permissions on the file we're considering grabbing
            if (!timingInfo.timeOperation(DiskOperation.CHECK_READABLE, () -> Files.isReadable(path))) {
                return false;
            }

            return !ignoreHidden || !timingInfo.timeOperation(DiskOperation.CHECK_HIDDEN, file::isHidden);
        };
    }
}
