package net.lingala.zip4j.io.inputstream;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.contentHandlers.B2ContentMemoryWriter;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2DownloadByIdRequest;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.util.B2ByteRange;

import java.io.*;
import java.math.BigInteger;
import java.security.ProviderException;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

import static net.lingala.zip4j.util.B2Utils.*;

public class B2RemoteRandomAccessFile implements Closeable {
    protected PrintWriter writer = new PrintWriter(System.err, true);

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
    protected B2File b2File;
    protected final String b2FileId;

    public final long size; // number of bytes in the file

    protected long bytePosition = 0; // file position, offset from the start of the file: 0 to file length - in bytes
    protected long lastBytePosition = 0; // most recent previous bytePosition
    protected void setBytePosition(long value) {
        lastBytePosition = bytePosition;
        bytePosition = Math.min(Math.max(value, 0), size - 1); // bound within file contents
//        writer.println("\tbytePosition: from " + NumberFormat.getInstance().format(lastBytePosition) + " to " + NumberFormat.getInstance().format(bytePosition));
    }
    protected final long blockSize = 64 * 1024 * 1024; // size of an individual block - in bytes

    protected long getBlockPosition() { // file position, offset from the start of the file: 0 to last block - in blocks
        return bytePosition / blockSize;
    }

    // key = block number, value = byte[blockSize]
    protected Hashtable<Long, byte[]> blocks = new Hashtable<Long, byte[]>(32); // cache of remote data
    protected int maxBlocks = 16; // maximum number of blocks to cache
    protected int blockSpillAmount = 4; // amount of blocks to discard when the maximum is reached
    // queue representing blocks in recent usage order: most recently used is the first, least recently used is the last
    protected LinkedList<Long> blockUsageQueue = new LinkedList<Long>();

    /**
     * Return the byte length of the file.
     * @return size of the file in bytes
     */
    public long length() { return size; }

    public B2RemoteRandomAccessFile(B2File file) throws FileNotFoundException, IOException {
        this.b2File = file;
        size = b2File.length();

        // ensure the file exists
        boolean foundFile = false;
        try { foundFile = file.exists(); } catch (Exception e) { throw new IOException(e); }
        if (!foundFile) { throw new FileNotFoundException("B2 file does not exist"); }

        if (!file.canRead()) { throw new IOException("B2 file is unreadable"); } // ensure the file is readable

        b2FileId = file.getB2FileVersion().getFileId(); // store the file id for quick access later
    }

    /**
     * Sets the file-pointer offset, measured from the beginning of this
     * file, at which the next read or write occurs.  The offset is bound
     * to stay within 0 and the file size.
     *
     * @param      pos   the offset position, measured in bytes from the
     *                   beginning of the file, at which to set the file
     *                   pointer.
     * @throws     IOException  if {@code pos} is less than
     *                          {@code 0} or if an I/O error occurs.
     */
    public void seek(long pos) {
//        writer.println("B2RemoteRandomAccessFile.seek(long " + NumberFormat.getInstance().format(pos) + ")");
        setBytePosition(pos);
    }

    protected void cacheBlocks(List<Long> blockNumbers) throws IOException {
        boolean blocksCached = false;
        for (Long blockNumber : blockNumbers) {
            if (!blocks.containsKey(blockNumber)) {
                cacheBlock(blockNumber);
                blocksCached = true;
            }
        }
        if (blocksCached) {
            writer.println("\tcache size = " + NumberFormat.getInstance().format(blocks.size())
                    + "[" + NumberFormat.getInstance().format(blocks.size() * blockSize) + " bytes]");
        }
    }

    protected void cacheBlock(Long blockNumber) throws IOException {
        if (blocks.containsKey(blockNumber)) return;

        long startByteNumberInclusive = blockNumber * blockSize;
        long endByteNumberInclusive = Math.min(startByteNumberInclusive + blockSize - 1, size - 1); // adjust to stay within file contents

        B2ByteRange readRange = B2ByteRange.between(startByteNumberInclusive, endByteNumberInclusive);
        if ((readRange.start < 0) || (readRange.end >= size) || (readRange.start >= size) || (readRange.end < 0)) {
            throw new EOFException("Attempt to read outside of file contents");
        }
        assert(readRange.start <= readRange.end);

        byte[] remoteBytes = readRange(readRange, false); // read remote data and don't move the byte position

        blocks.put(blockNumber, remoteBytes); // store data in cache
        if (!blockUsageQueue.contains(blockNumber)) { blockUsageQueue.addFirst(blockNumber); } // initialise monitoring for use of this blocks

//        writer.println("B2RemoteRandomAccessFile.cacheBlock(Long " + NumberFormat.getInstance().format(blockNumber)
//                + ") " + readRange.toString() + "[" + NumberFormat.getInstance().format(readRange.getNumberOfBytes()) + "]");

        if (blocks.size() > maxBlocks) { // spill the cache when it's full
            for (int count = 0; count < blockSpillAmount; count++) {
                if (blockUsageQueue.size() > 0) {
                    Long lastUsedBlock = blockUsageQueue.removeLast();
                    if (lastUsedBlock != null) { blocks.remove(lastUsedBlock); }
                }
            }
            writer.println("\t\tspilled cache: " + NumberFormat.getInstance().format(blocks.size())
                    + "[" + NumberFormat.getInstance().format(blocks.size() * blockSize) + " bytes]");
        }
    }

    protected ArrayList<Long> getBlocksContainingByteRange(B2ByteRange range) {
        // ensure the file has the bytes to read
        if ((range.start < 0) || (range.end >= size) || (range.start >= size) || (range.end < 0)) {
            throw new IndexOutOfBoundsException("range is outside the bounds of the file contents");
        }

        assert(range.start <= range.end);
        long firstBlockNumber = range.start / blockSize;
        long lastBlockNumber = range.end / blockSize;

        ArrayList<Long> result = new ArrayList<>();
        for (long count = firstBlockNumber; count <= lastBlockNumber; count++) { result.add(count); }
        assert(result.size() > 0);

        return result;
    }

    /**
     * Reads {@code b.length} bytes from this file into the byte
     * array, starting at the current file pointer.  The file position is moved by the amount read
     * This is backed by memory so only small buffers are acceptable
     *
     * @param  buffer the buffer into which the data is read.
     * @throws EOFException if this file reaches the end before reading all the bytes.
     * @throws IOException if an I/O error occurs or if attempting to read outside of file contents
     */
    public void readFully(byte[] buffer) throws IOException {
        // ensure the file has the bytes to read
        B2ByteRange readRange = B2ByteRange.between(bytePosition, bytePosition + buffer.length - 1);
        if ((readRange.start < 0) || (readRange.end >= size) || (readRange.start >= size) || (readRange.end < 0)) {
            throw new EOFException("Attempt to read outside of file contents");
        }
        assert(readRange.start <= readRange.end);
        assert(readRange.getNumberOfBytes() < Integer.MAX_VALUE);

        // find the blocks that contain the range of data and cache them
        ArrayList<Long> dataBlocks = getBlocksContainingByteRange(readRange);
        cacheBlocks(dataBlocks);

        if (dataBlocks.size() == 1) { // simple case for reading from cache: all data in the same block
            Long blockNumber = dataBlocks.get(0);
            byte[] cacheBytes = blocks.get(blockNumber);
            long cachePosition = readRange.start % blockSize; // adjust position to cater for reading from the cached block
            System.arraycopy(cacheBytes, (int)cachePosition, buffer, 0, buffer.length);

            setBytePosition(readRange.end + 1); // move the byte position

//            writer.println("Cache read: block #" + NumberFormat.getInstance().format(blockNumber) + " for "
//                    + readRange.toString() + "[adjusted offset:" + NumberFormat.getInstance().format(sourcePosition) + " read length:"
//                    + NumberFormat.getInstance().format(buffer.length) + "]");

            return;
        } else if (dataBlocks.size() == 2) { // spanning 2 blocks
            writer.print("\tmulti-block read of " + NumberFormat.getInstance().format(dataBlocks.size())
                    + " blocks:");
            for (Long block : dataBlocks) {
                writer.print(" " + NumberFormat.getInstance().format(block));
            }
            writer.println(".");

            // first block is from readRange.start to the end of the block that readRange.start is within
            B2ByteRange firstBlockRange = new B2ByteRange(readRange.start, dataBlocks.get(0) * blockSize + blockSize - 1);
            // second block is from the start of the block then readRange.end is within to readRange.end
            B2ByteRange secondBlockRange = new B2ByteRange(dataBlocks.get(1) * blockSize, readRange.end);

            byte[] cacheFirstBlock = blocks.get(dataBlocks.get(0));
            long cacheFirstBlockPosition = firstBlockRange.start % blockSize; // adjust position to cater for reading from the cached block
            System.arraycopy(cacheFirstBlock, (int)cacheFirstBlockPosition, buffer, 0, (int)firstBlockRange.getNumberOfBytes());

            byte[] cacheSecondBlock = blocks.get(dataBlocks.get(1));
            System.arraycopy(cacheSecondBlock, 0, buffer, (int)firstBlockRange.getNumberOfBytes(), (int)secondBlockRange.getNumberOfBytes());

            setBytePosition(readRange.end + 1); // move the byte position

            return;
        }

        // TODO multi-block spanning cache scenario

        byte[] remoteBytes = readRange(readRange);
        System.arraycopy(remoteBytes, 0, buffer, 0, remoteBytes.length);

        for (Long usedBlock : dataBlocks) { // monitor block usage; most recently used moves to the first
            int indexOfUsedBlock = blockUsageQueue.indexOf(usedBlock);
            if (indexOfUsedBlock > -1) { // an indexOfUsedBlock of -1 means it isn't in the blockUsageQueue
                blockUsageQueue.remove(indexOfUsedBlock);
                blockUsageQueue.addFirst(usedBlock); // move item to first in the list to indicate that it is recently used
            } else {
                blockUsageQueue.addFirst(usedBlock); // this should never happen
            }
        }
    }

    /**
     * Reads up to {@code b.length} bytes of data from this file
     * into an array of bytes.  The file position is moved by the amount read
     * This is backed by memory so only small buffers are acceptable
     *
     * @param      b   the buffer into which the data is read.
     * @return     the total number of bytes read into the buffer, or
     *             {@code -1} if there is no more data because the end of
     *             this file has been reached.
     * @throws     IOException If the first byte cannot be read for any reason
     *             other than end of file, or if the random access file has been closed,
     *             or if some other I/O error occurs.
     * @throws     NullPointerException If {@code b} is {@code null}.
     */
    public int read(byte[] b) throws IOException {
        long startByte = bytePosition;
        long endByte = Math.min(bytePosition + b.length - 1, size - 1); // only read to end of the file contents
        B2ByteRange readRange = B2ByteRange.between(startByte, endByte);
        byte[] remoteBytes = new byte[(int)readRange.getNumberOfBytes()];
        readFully(remoteBytes);
        System.arraycopy(remoteBytes, 0, b, 0, remoteBytes.length);
        return remoteBytes.length;
    }

    /**
     * Read up to (@code buffer.length} bytes.
     * @param   buffer the buffer into which the data is read.
     * @param   off   the start offset into the data array {@code b}.
     * @param   len   the number of bytes to read.
     * @return the total number of bytes read into the buffer, or
     *             {@code -1} if there is no more data because the end of
     *             this file has been reached.
     * @throws IOException
     */
    public int read(byte[] buffer, int off, int len) throws IOException {
        long startByte = bytePosition;
        long endByte = Math.min(bytePosition + len - 1, size - 1); // only read to end of the file contents
        B2ByteRange readRange = B2ByteRange.between(startByte, endByte);
        byte[] remoteBytes = new byte[(int)readRange.getNumberOfBytes()];
        readFully(remoteBytes);
        System.arraycopy(remoteBytes, 0, buffer, off, remoteBytes.length);
        return remoteBytes.length;
    }

        /**
         * Reads exactly {@code len} bytes from this file into the byte
         * array, starting at the current file pointer. The file position is moved by the amount read
         * This is backed by memory so only small buffers are acceptable
         *
         * @param   buffer the buffer into which the data is read.
         * @param   off   the start offset into the data array {@code b}.
         * @param   len   the number of bytes to read.
         * @throws  NullPointerException if {@code b} is {@code null}.
         * @throws  IndexOutOfBoundsException if {@code off} is negative,
         *                {@code len} is negative, or {@code len} is greater than
         *                {@code b.length - off}.
         * @throws  EOFException  if this file reaches the end before reading
         *                all the bytes.
         * @throws  IOException   if an I/O error occurs.
         */
    public void readFully(byte[] buffer, int off, int len) throws IOException {
        byte[] remoteBytes = new byte[len];
        readFully(remoteBytes);
        System.arraycopy(remoteBytes, 0, buffer, off, len);
    }

    protected byte[] readRange(B2ByteRange range) throws IOException {
        return readRange(range, true);
    }

    protected byte[] readRange(B2ByteRange range, boolean moveBytePosition) throws IOException {
        writer.println("B2RemoteRandomAccessFile.readRange(B2ByteRange " + range.toString()
                + ", boolean " + moveBytePosition + ") ["
                + NumberFormat.getInstance().format(range.getNumberOfBytes()) + "]");

        // initialise a remote connection
        B2StorageClient b2Client = getRemoteClient();

        // read the remote data into memory
        B2DownloadByIdRequest request = B2DownloadByIdRequest
                .builder(b2FileId)
                .setRange(range)
                .build();
        B2ContentMemoryWriter sink = B2ContentMemoryWriter.build();
        try {
            b2Client.downloadById(request, sink);
        } catch (B2Exception b2e) {
            throw new IOException("Failed to read remote file data", b2e);
        }

        if (moveBytePosition) { setBytePosition(range.end + 1); } // when caching the byte position isn't moved

        return sink.getBytes();
    }

    /**
     * Attempts to skip over {@code n} bytes of input discarding the
     * skipped bytes.
     * <p>
     *
     * This method may skip over some smaller number of bytes, possibly zero.
     * This may result from any of a number of conditions; reaching end of
     * file before {@code n} bytes have been skipped is only one
     * possibility. This method never throws an {@code EOFException}.
     * The actual number of bytes skipped is returned.  If {@code n}
     * is negative, no bytes are skipped.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @throws     IOException  if an I/O error occurs.
     */
    public int skipBytes(int n) throws IOException {
        writer.println("B2RemoteRandomAccessFile.skipBytes(int " + NumberFormat.getInstance().format(n) + ")");

        setBytePosition( Math.min(bytePosition + n, size - 1) ); // seek forward
        return (int) (bytePosition - lastBytePosition); // return the actual number of bytes skipped
    }

    @Override
    public void close() throws IOException {
        if (remoteClient != null) {
            remoteClient.close();
            remoteClient = null;
        }
    }
}
