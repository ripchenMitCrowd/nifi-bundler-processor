package org.example.processors.bundler.sftp;

import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.PermissionDeniedException;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class BundlingSFTPTransfer extends SFTPTransfer {

    private final ComponentLog logger;

    public BundlingSFTPTransfer(PropertyContext propertyContext, ComponentLog logger) {
        super(propertyContext, logger);
        this.logger = logger;
    }

    @Override
    public FlowFile getRemoteFile(String remoteFileName, FlowFile origFlowFile, ProcessSession session) throws ProcessException, IOException {
        final String baseName = FilenameUtils.getBaseName(remoteFileName);
        final File bundledZip = new File(baseName+".zip");

        var currentRemoteFilesList = this.getListing(false);

        logger.log(LogLevel.DEBUG, "Listing for {} yields {} files", remoteFileName, currentRemoteFilesList.size());
        currentRemoteFilesList.forEach(fileInfo -> logger.log(LogLevel.DEBUG, "filename: {}, path: {}", fileInfo.getFileName(), fileInfo.getFullPathFileName()));

        var remoteBundleFilesList = currentRemoteFilesList.stream()
                .filter(fileInfo -> fileInfo.getFileName().startsWith(baseName))
                .toList();
        if (remoteBundleFilesList.isEmpty()) {
            logger.log(LogLevel.ERROR, "No additional files could be found on the remote system to bundle with {}", remoteFileName);
            return origFlowFile;
        }

        logger.log(LogLevel.DEBUG, "Bundling {} files into {}", remoteBundleFilesList.size(), bundledZip);
        remoteBundleFilesList.forEach(fileInfo -> logger.log(LogLevel.DEBUG, "bundle part: filename: {}, path: {}", fileInfo.getFileName(), fileInfo.getFullPathFileName()));

        try (FileOutputStream fileOutputStream = new FileOutputStream(bundledZip);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
             ZipOutputStream zos = new ZipOutputStream(bufferedOutputStream)
        ) {
            for (FileInfo partFileInfo : remoteBundleFilesList) {
                ZipEntry zipEntry = new ZipEntry(partFileInfo.getName());
                zipEntry.setSize(bundledZip.length());
                zos.putNextEntry(zipEntry);
                downloadFile(remoteFileName, origFlowFile, partFileInfo.getFullPathFileName(), zos);
                zos.closeEntry();
            }
        } catch (IOException e) {
            logger.error("Could not bundle files into zip file {} due to {}; routing to failure", bundledZip.getAbsolutePath(), e.toString(), e);
            return origFlowFile;
        }

        FlowFile bundledFlowFile;
        try (FileInputStream fis = new FileInputStream(bundledZip)) {
            bundledFlowFile = session.importFrom(fis, origFlowFile);
        } catch (IOException e) {
            logger.error("Could not put flow file {} due to {}; routing to failure", bundledZip.getAbsolutePath(), e.toString(), e);
            return origFlowFile;
        }
        FileUtils.delete(bundledZip);

        final Map<String, String> attributes = new HashMap<>();
        String parentPath = bundledZip.getParent();
        if (parentPath != null) {
            attributes.put(CoreAttributes.PATH.key(), parentPath);
        }
        attributes.put(CoreAttributes.FILENAME.key(), bundledZip.getName());
        attributes.put("parts", remoteBundleFilesList.stream().map(FileInfo::getFullPathFileName).toList().toString().replaceAll("\\[","").replaceAll("]","").replaceAll(", ",","));
        return session.putAllAttributes(bundledFlowFile, attributes);
    }

    protected void downloadFile(String remoteFileName, FlowFile origFlowFile, String partName, ZipOutputStream zos) throws IOException {
        try (final RemoteFile rf = getSFTPClient(origFlowFile).open(partName);
             final RemoteFile.ReadAheadRemoteFileInputStream aheadRemoteFileInputStream =  rf.new ReadAheadRemoteFileInputStream(16)
        ) {
            StreamUtils.copy(aheadRemoteFileInputStream, zos);
        } catch (final SFTPException e) {
            switch (e.getStatusCode()) {
                case NO_SUCH_FILE:
                    throw new FileNotFoundException("Could not find file " + remoteFileName + " on remote SFTP Server");
                case PERMISSION_DENIED:
                    throw new PermissionDeniedException("Insufficient permissions to read file " + remoteFileName + " from remote SFTP Server", e);
                default:
                    throw new IOException("Failed to obtain file content for " + remoteFileName, e);
            }
        }
    }
}
