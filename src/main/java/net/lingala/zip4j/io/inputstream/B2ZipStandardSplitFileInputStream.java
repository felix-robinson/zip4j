package net.lingala.zip4j.io.inputstream;

import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.enums.RandomAccessFileMode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class B2ZipStandardSplitFileInputStream extends SplitFileInputStream {
    protected B2RemoteRandomAccessFile randomAccessFile;

    protected B2File zipFile;
    private int lastSplitZipFileNumber;
    private boolean isSplitZipArchive;
    private int currentSplitFileCounter = 0;
    private byte[] singleByteArray = new byte[1];

    public B2ZipStandardSplitFileInputStream(B2File b2File, boolean isSplitZipArchive, int lastSplitZipFileNumber) throws FileNotFoundException, IOException {
        this.randomAccessFile = new B2RemoteRandomAccessFile(b2File);
        this.zipFile = b2File;
        this.isSplitZipArchive = isSplitZipArchive;
        this.lastSplitZipFileNumber = lastSplitZipFileNumber;

        if (isSplitZipArchive) {
            currentSplitFileCounter = lastSplitZipFileNumber;
        }
    }

    @Override
    public int read() throws IOException {
        int readLen = read(singleByteArray);
        if (readLen == -1) {
            return -1;
        }

        return singleByteArray[0];
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int readLen = randomAccessFile.read(b, off, len);

        if ((readLen != len || readLen == -1) && isSplitZipArchive) {
            openRandomAccessFileForIndex(currentSplitFileCounter + 1);
            currentSplitFileCounter++;

            if (readLen < 0) readLen = 0;
            int newlyRead = randomAccessFile.read(b, readLen, len - readLen);
            if (newlyRead > 0) readLen += newlyRead;
        }

        return readLen;
    }

    @Override
    public void prepareExtractionForFileHeader(FileHeader fileHeader) throws IOException {

        if (isSplitZipArchive && (currentSplitFileCounter != fileHeader.getDiskNumberStart())) {
            openRandomAccessFileForIndex(fileHeader.getDiskNumberStart());
            currentSplitFileCounter = fileHeader.getDiskNumberStart();
        }

        randomAccessFile.seek(fileHeader.getOffsetLocalHeader());
    }

    @Override
    public void close() throws IOException {
        if (randomAccessFile != null) {
            randomAccessFile.close();
        }
    }

    protected void openRandomAccessFileForIndex(int zipFileIndex) throws IOException {
        B2File nextSplitFile = getNextSplitFile(zipFileIndex);
        if (!nextSplitFile.exists()) {
            throw new FileNotFoundException("zip split file does not exist: " + nextSplitFile);
        }
        randomAccessFile.close();
        randomAccessFile = new B2RemoteRandomAccessFile(nextSplitFile);
    }

    protected B2File getNextSplitFile(int zipFileIndex) throws IOException {
        if (zipFileIndex == lastSplitZipFileNumber) {
            return zipFile;
        }

        String currZipFileNameWithPath = zipFile.getCanonicalPath();
        String extensionSubString = ".z0";
        if (zipFileIndex >= 9) {
            extensionSubString = ".z";
        }

        return new B2File(zipFile.getB2Bucket(), currZipFileNameWithPath.substring(0,
                        currZipFileNameWithPath.lastIndexOf(".")) + extensionSubString + (zipFileIndex + 1));
    }

}
