package net.lingala.zip4j.io.inputstream;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.util.B2ByteRange;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.security.ProviderException;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;

import static net.lingala.zip4j.util.B2Utils.*;

public class B2File extends File {
    private B2Bucket b2Bucket;
    public B2Bucket getB2Bucket() { return b2Bucket; }
    private B2FileVersion b2File;
    public B2FileVersion getB2FileVersion() { return b2File; }

    protected B2StorageClient remoteClient = null;
    protected B2StorageClient getRemoteClient() throws IOException {
        if (remoteClient == null) {
            B2StorageClient client = null;
            try {
                // throws RunTimeException when remoteClient can't be created
                client = B2StorageClientFactory
                        .createDefaultFactory()
                        .create(APPLICATION_KEY_ID, APPLICATION_KEY, USER_AGENT);
            } catch (Exception e) {
                throw new IOException("Failed to create B2StorageClient", e);
            }
            remoteClient = client;
        }

        return remoteClient;
    }

    public B2File(B2Bucket bucket, String filename) throws FileNotFoundException, IOException {
        super(bucket.getBucketName(), filename);

        assert(bucket != null);
        assert(filename != null);
        assert(!filename.isEmpty());

        b2Bucket = bucket;
        b2File = b2fileVersionFromFileName(filename);
    }

    /**
     * Create a B2File from B2 bucket and B2 file details.
     * @param bucket B2 bucket that file is contained within
     * @param file B2 file
     */
    public B2File(B2Bucket bucket, B2FileVersion file) {
        super(bucket.getBucketName(), file.getFileName());

        assert(bucket != null);
        assert(file != null);
        b2Bucket = bucket;
        b2File = file;
    }

    protected B2FileVersion b2fileVersionFromFileName(String filename) throws FileNotFoundException, IOException {
        B2StorageClient b2Client = getRemoteClient();
        assert(b2Client != null);

        List<B2Bucket> remoteBuckets = null;
        try {
            // throws B2Exception
            remoteBuckets = b2Client.buckets();
        } catch (B2Exception b2e) {
            throw new IOException("Failed to retrieve B2 bucket list", b2e);
        }
        assert (remoteBuckets != null);

        boolean foundBucket = remoteBuckets.stream()
                .filter( streamElement -> b2Bucket.getBucketId().equals(streamElement.getBucketId()) )
                .findAny()
                .orElse(null)
                != null;
        if (!foundBucket) { // remote bucket not found
            throw new FileNotFoundException("bucket not found: " + b2Bucket.getBucketName());
        }

        B2ListFilesIterable remoteFiles = null;
        try {
            // throws B2Exception
            remoteFiles = b2Client.fileNames(b2Bucket.getBucketId());
        } catch (B2Exception b2e) {
            throw new IOException("Failed to retrieve B2 file list", b2e);
        }
        assert (remoteFiles != null);

        Iterator<B2FileVersion> remoteFilesIterator = remoteFiles.iterator();
        B2FileVersion remoteFile = null;
        while (remoteFilesIterator.hasNext()) {
            remoteFile = remoteFilesIterator.next();
            if (filename.equals(remoteFile.getFileName())) { return remoteFile; }
        }

        throw new FileNotFoundException("file not found " + filename);
    }

    /**
     * Determine if the B2File is executable.  Always returns false; B2 is unable to remotely execute files.
     * @return false, indicating that the file cannot be executed
     */
    @Override
    public boolean canExecute() {
        return false;
    }

    /**
     * Determine if the B2File is readable.
     * @return true, indicating that the file is readable; otherwise, false
     */
    @Override
    public boolean canRead() {
        try {
            return exists();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Determine if the B2File is writable.  Always returns false; this class does not support writing to Be Files.
     * @return false, indicating that the file is not writable
     */
    @Override
    public boolean canWrite() {
        return false;
    }

    // No need to override, compares abstract path names only.
//    public int compareTo(File pathname) {

    /**
     * Atomically create a new file in B2 storage.  Always throws an UnsupportedOperationException because B2 file
     * creation is unsupported.
     * @return never returns, always throws UnsupportedOperationException
     * @throws IOException never throws IOException, always throws UnsupportedOperationException
     */
    @Override
    public boolean createNewFile() throws IOException {
        throw new UnsupportedOperationException("B2 file creation is unsupported.");
    }

    /**
     * Deletes the B2 file.  Always returns false because B2 file deletion is unsupported.
     * @return false, indicating that the deletion did not occur
     */
    @Override
    public boolean delete() {
        return false;
    }

    /**
     * Deletes the B2 file when the virtual machine terminates.  Always throws an UnsupportedOperationException because
     * B2 file deletion is unsupported.
     * @throws UnsupportedOperationException always throws UnsupportedOperationException
     */
    @Override
    public void deleteOnExit() {
        throw new UnsupportedOperationException("deleteOnExit operation is unsupported for B2 storage.");
    }

    // No need to override, uses compareTo()
//    public boolean equals(Object obj) {

    /**
     * Determines if the file exists in B2.
     * @return true if the file exists, false otherwise
     * @throws ProviderException when a B2 storage client cannot be created
     * @throws UncheckedIOException when unable to retrieve remote B2 bucket/file details
     */
    @Override
    public boolean exists() {
        B2StorageClient b2Client = null;
        try { b2Client = getRemoteClient(); } catch (IOException ioe) { return false; }
        assert(b2Client != null);

        List<B2Bucket> remoteBuckets = null;
        try {
            // throws B2Exception
            remoteBuckets = b2Client.buckets();
        } catch (B2Exception b2e) {
            throw new UncheckedIOException(new IOException("Failed to retrieve B2 bucket list", b2e));
        }
        assert (remoteBuckets != null);

        boolean foundBucket = remoteBuckets.stream()
                .filter( streamElement -> b2Bucket.getBucketId().equals(streamElement.getBucketId()) )
                .findAny()
                .orElse(null)
                != null;
        if (!foundBucket) { // remote bucket not found
            return false;
        }

        B2ListFilesIterable remoteFiles = null;
        try {
            // throws B2Exception
            remoteFiles = b2Client.fileNames(b2Bucket.getBucketId());
        } catch (B2Exception b2e) {
            throw new UncheckedIOException(new IOException("Failed to retrieve B2 file list", b2e));
        }
        assert (remoteFiles != null);

        Iterator<B2FileVersion> remoteFilesIterator = remoteFiles.iterator();
        while (remoteFilesIterator.hasNext()) {
            if (b2File.getFileId().equals(remoteFilesIterator.next().getFileId())) return true;
        }

        PrintWriter writer = new PrintWriter(System.err, true);
        writer.println("B2File.exists()");

        return false;
    }

    /**
     * Returns the absolute form of this filename.
     * @return clone of this object
     */
    @Override
    public File getAbsoluteFile() {
        return new B2File(b2Bucket, b2File);
    }

    /**
     * Returns the absolute form of this pathname.
     * @return the b2 file id
     */
    @Override
    public String getAbsolutePath() {
        return b2File.getFileName();
    }

    /**
     * Returns the canonical form of this filename.
     * @return clone of this object
     */
    @Override
    public File getCanonicalFile() throws IOException {
        return new B2File(b2Bucket, b2File);
    }

    /**
     * Returns the canonical form of this pathname.
     * @return the B2 file id
     */
    @Override
    public String getCanonicalPath() throws IOException {
        return b2File.getFileName();
    }

    /**
     * Determine the amount of free bytes in the File location/partition.  B2 storage is unbounded and cannot provide
     * this value.
     * @return 0 always, because this number cannot be obtained.
     */
    @Override
    public long getFreeSpace() {
        return 0;
    }

    // No need to override, uses file and path Strings
//    public String getName() {

    // No need to override, uses file and path Strings
//    public String getParent() {

    /**
     * Returns the abstract pathname of this abstract pathname's parent, or null if this pathname does not name a
     * parent directory. Always returns null because B2 parent pathnames are unsupported.
     * @return null, always because parent pathnames are unsupported
     */
    @Override
    public File getParentFile() {
        return null;
    }

    // No need to override, uses path String
//    public String getPath() {

    /**
     * Determine the amount of bytes allocated to the storage location.  B2 storage is unbounded and cannot provide
     * this value.
     * @return 0 always, because this number cannot be obtained.
     */
    @Override
    public long getTotalSpace() {
        return 0;
    }

    /**
     * Determine the amount of bytes usable in the storage location.  B2 storage is unbounded and cannot provide
     * this value.
     * @return 0 always, because this number cannot be obtained.
     */
    @Override
    public long getUsableSpace() {
        return 0;
    }

    // No need to overwrite, uses path String
//    public int hashCode() {

    // No need to overwrite, uses prefix String
//    public boolean isAbsolute() {

    /**
     * Tests whether the file denoted by this abstract pathname is a directory.
     * @return true if this object represents a B2 folder
     */
    @Override
    public boolean isDirectory() {
        return b2File.isFolder();
    }

    /**
     * Tests whether the file denoted by this abstract pathname is a file.
     * @return true if this object represents a B2 file
     */
    public boolean isFile() {
        return !b2File.isFolder();
    }

    /**
     * Tests whether the file denoted by this abstrct pathname is a hidden file/folder.
     * @return true if this objects represents a hidden B2 file/folder
     */
    public boolean isHidden() {
        return b2File.isHide();
    }

    /**
     * Returns the time that the file denoted by this abstract pathname was last modified.
     * @return A long value representing the time the file was last modified.
     */
    @Override
    public long lastModified() {
        return b2File.getUploadTimestamp();
    }

    /**
     * Returns the length of the file denoted by this abstract pathname.
     * @return he length, in bytes, of the file denoted by this abstract pathname.
     */
    public long length() {
        return b2File.getContentLength();
    }

    /**
     * Returns an array of strings naming the files and directories in the directory denoted by this abstract pathname.
     * If this abstract pathname does not denote a directory, then this method returns null.  Directory listing is
     * unsupported so either an empty array or null is always returned.
     * @return An array of strings naming the files and directories in the directory denoted by this abstract pathname.
     * The array will be empty if the directory is empty. Returns null if this abstract pathname does not denote a
     * directory, or if an I/O error occurs.
     */
    @Override
    public String[] list() {
        if ( isDirectory() ) { return new String[0]; }
        return null;
    }

    /**
     * Returns an array of strings naming the files and directories in the directory denoted by this abstract pathname
     * that satisfy the specified filter.  Directory listing is unsupported so either an empty array or null is always
     * returned.
     * @param filter
     *         A filename filter
     *
     * @return An array of strings naming the files and directories in the directory denoted by this abstract pathname
     * that were accepted by the given filter. The array will be empty if the directory is empty or if no names were
     * accepted by the filter. Returns null if this abstract pathname does not denote a directory, or if an I/O error
     * occurs
     */
    @Override
    public String[] list(FilenameFilter filter) {
        if ( isDirectory() ) { return new String[0]; }
        return null;
    }

    /**
     * Returns an array of abstract pathnames denoting the files in the directory denoted by this abstract pathname.
     * Directory listing is unsupported so either an empty array or null is always returned.
     * @return An array of abstract pathnames denoting the files and directories in the directory denoted by this
     * abstract pathname. The array will be empty if the directory is empty. Returns null if this abstract pathname
     * does not denote a directory, or if an I/O error occurs.
     */
    @Override
    public File[] listFiles() {
        if ( isDirectory() ) { return new B2File[0]; }
        return null;
    }

    /**
     * Returns an array of abstract pathnames denoting the files and directories in the directory denoted by this
     * abstract pathname that satisfy the specified filter.  Directory listing is unsupported so either an empty array
     * or null is always returned.
     * @param filter
     *         A file filter
     *
     * @return An array of abstract pathnames denoting the files and directories in the directory denoted by this
     * abstract pathname. The array will be empty if the directory is empty. Returns null if this abstract pathname
     * does not denote a directory, or if an I/O error occurs.
     */
    @Override
    public File[] listFiles(FileFilter filter) {
        if ( isDirectory() ) { return new B2File[0]; }
        return null;
    }

    /**
     * Returns an array of abstract pathnames denoting the files and directories in the directory denoted by this
     * abstract pathname that satisfy the specified filter.  Directory listing is unsupported so either an empty array
     * or null is always returned.
     * @param filter
     *         A filename filter
     *
     * @return An array of abstract pathnames denoting the files and directories in the directory denoted by this
     * abstract pathname. The array will be empty if the directory is empty. Returns null if this abstract pathname
     * does not denote a directory, or if an I/O error occurs.
     */
    @Override
    public File[] listFiles(FilenameFilter filter) {
        if ( isDirectory() ) { return new B2File[0]; }
        return null;
    }

    // can't override static methods
//    public static File[] listRoots() {

    /**
     * Creates the directory named by this abstract pathname.  Directory creation is unsupported - always returns false.
     * @return true if and only if the directory was created; false otherwise
     */
    @Override
    public boolean mkdir() {
        return false;
    }

    /**
     * Creates the directory named by this abstract pathname, including any necessary but nonexistent parent
     * directories.  Directory creation is unsupported - always returns false.
     * @return true if and only if the directory was created, along with all necessary parent directories; false
     * otherwise
     */
    @Override
    public boolean mkdirs() {
        return false;
    }

    /**
     * Renames the file denoted by this abstract pathname.  Renaming is unsupported - always returns false.
     * @param dest  The new abstract pathname for the named file
     *
     * @return true if and only if the renaming succeeded; false otherwise
     */
    @Override
    public boolean renameTo(File dest) {
        return false;
    }

    /**
     * A convenience method to set the owner's execute permission for this abstract pathname. Execute permission is
     * unsupported - always returns false.
     * @param executable
     *          If {@code true}, sets the access permission to allow execute
     *          operations; if {@code false} to disallow execute operations
     *
     * @return true if and only if the operation succeeded. The operation will fail if the user does not have
     * permission to change the access permissions of this abstract pathname. If executable is false and the
     * underlying file system does not implement an execute permission, then the operation will fail.
     */
    @Override
    public boolean setExecutable(boolean executable) {
        return false;
    }

    /**
     * Sets the owner's or everybody's execute permission for this abstract pathname.  Execute permission is
     * unsupported - always returns false.
     * @param executable
     *          If {@code true}, sets the access permission to allow execute
     *          operations; if {@code false} to disallow execute operations
     *
     * @param ownerOnly
     *          If {@code true}, the execute permission applies only to the
     *          owner's execute permission; otherwise, it applies to everybody.
     *          If the underlying file system can not distinguish the owner's
     *          execute permission from that of others, then the permission will
     *          apply to everybody, regardless of this value.
     *
     * @return true if and only if the operation succeeded. The operation will fail if the user does not have
     * permission to change the access permissions of this abstract pathname. If executable is false and the
     * underlying file system does not implement an execute permission, then the operation will fail.
     */
    @Override
    public boolean setExecutable(boolean executable, boolean ownerOnly) {
        return false;
    }

    /**
     * Sets the last-modified time of the file or directory named by this abstract pathname.  Setting modified time is
     * unsupported - always returns false.
     * @param time  The new last-modified time, measured in milliseconds since
     *               the epoch (00:00:00 GMT, January 1, 1970)
     *
     * @return true if and only if the operation succeeded; false otherwise
     */
    @Override
    public boolean setLastModified(long time) {
        return false;
    }

    /**
     * A convenience method to set the owner's read permission for this abstract pathname.  Setting readability is
     * unsupported - always returns false.
     * @param readable
     *          If {@code true}, sets the access permission to allow read
     *          operations; if {@code false} to disallow read operations
     *
     * @return true if and only if the operation succeeded. The operation will fail if the user does not have
     * permission to change the access permissions of this abstract pathname. If readable is false and the underlying
     * file system does not implement a read permission, then the operation will fail.
     */
    @Override
    public boolean setReadable(boolean readable) {
        return false;
    }

    /**
     * Sets the owner's or everybody's read permission for this abstract pathname.  Setting readability is
     * unsupported - always returns false.
     * @param readable
     *          If {@code true}, sets the access permission to allow read
     *          operations; if {@code false} to disallow read operations
     *
     * @param ownerOnly
     *          If {@code true}, the read permission applies only to the
     *          owner's read permission; otherwise, it applies to everybody.  If
     *          the underlying file system can not distinguish the owner's read
     *          permission from that of others, then the permission will apply to
     *          everybody, regardless of this value.
     *
     * @return true if and only if the operation succeeded. The operation will fail if the user does not have
     * permission to change the access permissions of this abstract pathname. If readable is false and the underlying
     * file system does not implement a read permission, then the operation will fail.
     */
    @Override
    public boolean setReadable(boolean readable, boolean ownerOnly) {
        return false;
    }

    /**
     * Marks the file or directory named by this abstract pathname so that only read operations are allowed.  Setting
     * readability is unsupported - always returns false.
     * @return true if and only if the operation succeeded; false otherwise
     */
    @Override
    public boolean setReadOnly() {
        return false;
    }

    /**
     * A convenience method to set the owner's write permission for this abstract pathname.  Setting write ability is
     * unsupported - always returns false.
     * @param writable
     *          If {@code true}, sets the access permission to allow write
     *          operations; if {@code false} to disallow write operations
     *
     * @return true if and only if the operation succeeded. The operation will fail if the user does not have
     * permission to change the access permissions of this abstract pathname.
     */
    @Override
    public boolean setWritable(boolean writable) {
        return false;
    }

    /**
     * Sets the owner's or everybody's write permission for this abstract pathname.  Setting write ability is
     * unsupported - always returns false
     * @param writable
     *          If {@code true}, sets the access permission to allow write
     *          operations; if {@code false} to disallow write operations
     *
     * @param ownerOnly
     *          If {@code true}, the write permission applies only to the
     *          owner's write permission; otherwise, it applies to everybody.  If
     *          the underlying file system can not distinguish the owner's write
     *          permission from that of others, then the permission will apply to
     *          everybody, regardless of this value.
     *
     * @return true if and only if the operation succeeded. The operation will fail if the user does not have
     * permission to change the access permissions of this abstract pathname.
     */
    @Override
    public boolean setWritable(boolean writable, boolean ownerOnly) {
        return false;
    }

    /**
     * Returns a java.nio.file.Path object constructed from this abstract path.  Converting to a Path is unsupported.
     * Allways throws an UnsupportedOperationException
     * @return
     * @throws UnsupportedOperationException always because converting to a Path is unsupported
     */
    @Override
    public Path toPath() {
        throw new UnsupportedOperationException("toPath operation is not supported for B2");
    }

    // No need to override, uses path String
//    public String toString() {

    /**
     * Constructs a file: URI that represents this abstract pathname.  Generating a URI is unsupported.  Always throws
     * an UnsupportedOperationException
     * @return
     * @throws UnsupportedOperationException always because generating a URI is unsupported
     */
    @Override
    public URI toURI() {
        throw new UnsupportedOperationException("toURI operation is unsupported for B2");
    }
}
