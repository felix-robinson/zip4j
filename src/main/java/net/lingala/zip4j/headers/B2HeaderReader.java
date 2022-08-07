package net.lingala.zip4j.headers;

import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.inputstream.B2RemoteRandomAccessFile;
import net.lingala.zip4j.io.inputstream.NumberedSplitRandomAccessFile;
import net.lingala.zip4j.model.*;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.model.enums.EncryptionMethod;
import net.lingala.zip4j.util.RawIO;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static net.lingala.zip4j.headers.HeaderUtil.decodeStringWithCharset;
import static net.lingala.zip4j.util.BitUtils.isBitSet;
import static net.lingala.zip4j.util.InternalZipConstants.*;
import static net.lingala.zip4j.util.Zip4jUtil.readFully;

/**
 * Header Reader extended to support B2 remote file storage.  The methods in HeaderReader
 * are tightly coupled to RandomAccessFile so B2HeaderReader adds new methods that use
 * a B2RemoteRandomAccessFile.
 */
public class B2HeaderReader extends HeaderReader {
// No need to override isDirectory(); it will work with B2HeaderReader data
//    public boolean isDirectory(byte[] externalFileAttributes, String fileName) {

//    private ZipModel zipModel;
//    private final RawIO rawIO = new RawIO();
//    private final byte[] intBuff = new byte[4];


    /**
     * Configure ZipModel with information from all the zip file headers
     * @param b2Rraf zip file in B2
     * @param zip4jConfig configuration
     * @return ZipModel populated with sip file header information
     * @throws IOException if unable to read the zip file content
     */
    public ZipModel
    readAllHeaders(B2RemoteRandomAccessFile b2Rraf, Zip4jConfig zip4jConfig)
            throws IOException {

        if (b2Rraf.length() < ENDHDR) {
            throw new ZipException("Zip file size less than minimum expected zip file size. " +
                    "Probably not a zip file or a corrupted zip file");
        }

        zipModel = new ZipModel();
        try {
            EndOfCentralDirectoryRecord endOfCentralDirectoryRecord = readEndOfCentralDirectoryRecord(b2Rraf, rawIO, zip4jConfig);
            zipModel.setEndOfCentralDirectoryRecord(endOfCentralDirectoryRecord);
        } catch (ZipException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw new ZipException("Zip headers not found. Probably not a zip file or a corrupted zip file", e);
        }

        if (zipModel.getEndOfCentralDirectoryRecord().getTotalNumberOfEntriesInCentralDirectory() == 0) {
            return zipModel;
        }

        // If file is Zip64 format, Zip64 headers have to be read before reading central directory
        long offsetOfEneOfCentralDirectory = zipModel.getEndOfCentralDirectoryRecord().getOffsetOfEndOfCentralDirectory();
        Zip64EndOfCentralDirectoryLocator zip64EndOfCentralDirectoryLocator = readZip64EndOfCentralDirectoryLocator(b2Rraf, rawIO, offsetOfEneOfCentralDirectory);
        zipModel.setZip64EndOfCentralDirectoryLocator(zip64EndOfCentralDirectoryLocator);

        if (zipModel.isZip64Format()) {
            Zip64EndOfCentralDirectoryRecord zip64EndCentralDirRec = readZip64EndCentralDirRec(b2Rraf, rawIO);
            zipModel.setZip64EndOfCentralDirectoryRecord(zip64EndCentralDirRec);
            Zip64EndOfCentralDirectoryRecord zip64EndOfCentralDirectoryRecord = zipModel.getZip64EndOfCentralDirectoryRecord();
            if (zip64EndOfCentralDirectoryRecord != null && zip64EndOfCentralDirectoryRecord.getNumberOfThisDisk() > 0) {
                zipModel.setSplitArchive(true);
            } else {
                zipModel.setSplitArchive(false);
            }
        }

        CentralDirectory centralDirectory = readCentralDirectory(b2Rraf, rawIO, zip4jConfig.getCharset());
        zipModel.setCentralDirectory(centralDirectory);

        return zipModel;
    }

    @Override
    public DataDescriptor
    readDataDescriptor(InputStream inputStream, boolean isZip64Format)
            throws IOException {

        return null;
        /*
        DataDescriptor dataDescriptor = new DataDescriptor();

        byte[] intBuff = new byte[4];
        readFully(inputStream, intBuff);
        long sigOrCrc = rawIO.readLongLittleEndian(intBuff, 0);

        //According to zip specification, presence of extra data record header signature is optional.
        //If this signature is present, read it and read the next 4 bytes for crc
        //If signature not present, assign the read 4 bytes for crc
        if (sigOrCrc == HeaderSignature.EXTRA_DATA_RECORD.getValue()) {
            dataDescriptor.setSignature(HeaderSignature.EXTRA_DATA_RECORD);
            readFully(inputStream, intBuff);
            dataDescriptor.setCrc(rawIO.readLongLittleEndian(intBuff, 0));
        } else {
            dataDescriptor.setCrc(sigOrCrc);
        }

        if (isZip64Format) {
            dataDescriptor.setCompressedSize(rawIO.readLongLittleEndian(inputStream));
            dataDescriptor.setUncompressedSize(rawIO.readLongLittleEndian(inputStream));
        } else {
            dataDescriptor.setCompressedSize(rawIO.readIntLittleEndian(inputStream));
            dataDescriptor.setUncompressedSize(rawIO.readIntLittleEndian(inputStream));
        }

        return dataDescriptor;
        */
    }

    @Override
    public LocalFileHeader
    readLocalFileHeader(InputStream inputStream, Charset charset)
            throws IOException {

        return null;
        /*
        LocalFileHeader localFileHeader = new LocalFileHeader();
        byte[] intBuff = new byte[4];

        //signature
        int sig = rawIO.readIntLittleEndian(inputStream);
        if (sig == HeaderSignature.TEMPORARY_SPANNING_MARKER.getValue()) {
            sig = rawIO.readIntLittleEndian(inputStream);
        }
        if (sig != HeaderSignature.LOCAL_FILE_HEADER.getValue()) {
            return null;
        }
        localFileHeader.setSignature(HeaderSignature.LOCAL_FILE_HEADER);
        localFileHeader.setVersionNeededToExtract(rawIO.readShortLittleEndian(inputStream));

        byte[] generalPurposeFlags = new byte[2];
        if (readFully(inputStream, generalPurposeFlags) != 2) {
            throw new ZipException("Could not read enough bytes for generalPurposeFlags");
        }
        localFileHeader.setEncrypted(isBitSet(generalPurposeFlags[0], 0));
        localFileHeader.setDataDescriptorExists(isBitSet(generalPurposeFlags[0], 3));
        localFileHeader.setFileNameUTF8Encoded(isBitSet(generalPurposeFlags[1], 3));
        localFileHeader.setGeneralPurposeFlag(generalPurposeFlags.clone());

        localFileHeader.setCompressionMethod(CompressionMethod.getCompressionMethodFromCode(
                rawIO.readShortLittleEndian(inputStream)));
        localFileHeader.setLastModifiedTime(rawIO.readIntLittleEndian(inputStream));

        readFully(inputStream, intBuff);
        localFileHeader.setCrc(rawIO.readLongLittleEndian(intBuff, 0));

        localFileHeader.setCompressedSize(rawIO.readLongLittleEndian(inputStream, 4));
        localFileHeader.setUncompressedSize(rawIO.readLongLittleEndian(inputStream, 4));

        int fileNameLength = rawIO.readShortLittleEndian(inputStream);
        localFileHeader.setFileNameLength(fileNameLength);

        localFileHeader.setExtraFieldLength(rawIO.readShortLittleEndian(inputStream));

        if (fileNameLength > 0) {
            byte[] fileNameBuf = new byte[fileNameLength];
            readFully(inputStream, fileNameBuf);

            String fileName = decodeStringWithCharset(fileNameBuf, localFileHeader.isFileNameUTF8Encoded(), charset);
            localFileHeader.setFileName(fileName);
            localFileHeader.setDirectory(fileName.endsWith("/") || fileName.endsWith("\\"));
        } else {
            throw new ZipException("Invalid entry name in local file header");
        }

        readExtraDataRecords(inputStream, localFileHeader);
        readZip64ExtendedInfo(localFileHeader, rawIO);
        readAesExtraDataRecord(localFileHeader, rawIO);

        if (localFileHeader.isEncrypted()) {

            if (localFileHeader.getEncryptionMethod() == EncryptionMethod.AES) {
                //Do nothing
            } else {
                if (isBitSet(localFileHeader.getGeneralPurposeFlag()[0], 6)) {
                    localFileHeader.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD_VARIANT_STRONG);
                } else {
                    localFileHeader.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
                }
            }

        }

        return localFileHeader;
        */
    }

    private EndOfCentralDirectoryRecord
    readEndOfCentralDirectoryRecord(B2RemoteRandomAccessFile b2Rraf, RawIO rawIO, Zip4jConfig zip4jConfig)
            throws IOException {

        long offsetEndOfCentralDirectory = locateOffsetOfEndOfCentralDirectory(b2Rraf);
        seekInCurrentPart(b2Rraf, offsetEndOfCentralDirectory + 4);

        EndOfCentralDirectoryRecord endOfCentralDirectoryRecord = new EndOfCentralDirectoryRecord();
        endOfCentralDirectoryRecord.setSignature(HeaderSignature.END_OF_CENTRAL_DIRECTORY);
        endOfCentralDirectoryRecord.setNumberOfThisDisk(rawIO.readShortLittleEndian(b2Rraf));
        endOfCentralDirectoryRecord.setNumberOfThisDiskStartOfCentralDir(rawIO.readShortLittleEndian(b2Rraf));
        endOfCentralDirectoryRecord.setTotalNumberOfEntriesInCentralDirectoryOnThisDisk(
                rawIO.readShortLittleEndian(b2Rraf));
        endOfCentralDirectoryRecord.setTotalNumberOfEntriesInCentralDirectory(rawIO.readShortLittleEndian(b2Rraf));
        endOfCentralDirectoryRecord.setSizeOfCentralDirectory(rawIO.readIntLittleEndian(b2Rraf));
        endOfCentralDirectoryRecord.setOffsetOfEndOfCentralDirectory(offsetEndOfCentralDirectory);

        b2Rraf.readFully(intBuff);
        endOfCentralDirectoryRecord.setOffsetOfStartOfCentralDirectory(rawIO.readLongLittleEndian(intBuff, 0));

        int commentLength = rawIO.readShortLittleEndian(b2Rraf);
        endOfCentralDirectoryRecord.setComment(readZipComment(b2Rraf, commentLength, zip4jConfig.getCharset()));

        zipModel.setSplitArchive(endOfCentralDirectoryRecord.getNumberOfThisDisk() > 0);

        assert(zipModel.isSplitArchive() == false); // not supporting split archives!

        return endOfCentralDirectoryRecord;
    }

    protected long
    locateOffsetOfEndOfCentralDirectory(B2RemoteRandomAccessFile randomAccessFile)
            throws IOException {

        long zipFileSize = randomAccessFile.length();
        if (zipFileSize < ENDHDR) {
            throw new ZipException("Zip file size less than size of zip headers. Probably not a zip file.");
        }

        seekInCurrentPart(randomAccessFile, zipFileSize - ENDHDR);
        if (rawIO.readIntLittleEndian(randomAccessFile) == HeaderSignature.END_OF_CENTRAL_DIRECTORY.getValue()) {
            return zipFileSize - ENDHDR;
        }

        return locateOffsetOfEndOfCentralDirectoryByReverseSeek(randomAccessFile);
    }

    protected void
    seekInCurrentPart(B2RemoteRandomAccessFile randomAccessFile, long pos)
            throws IOException {

        // Not supporting multi-part zip files
//        if (randomAccessFile instanceof NumberedSplitRandomAccessFile) {
//            ((NumberedSplitRandomAccessFile) randomAccessFile).seekInCurrentPart(pos);
//        } else {
            randomAccessFile.seek(pos);
//        }
    }

    protected long
    locateOffsetOfEndOfCentralDirectoryByReverseSeek(B2RemoteRandomAccessFile randomAccessFile)
            throws IOException {

        long currentFilePointer = randomAccessFile.length() - ENDHDR;
        // reverse seek for a maximum of MAX_COMMENT_SIZE bytes
        long numberOfBytesToRead = randomAccessFile.length() < MAX_COMMENT_SIZE ? randomAccessFile.length() : MAX_COMMENT_SIZE;

        while (numberOfBytesToRead > 0 && currentFilePointer > 0) {
            seekInCurrentPart(randomAccessFile, --currentFilePointer);
            if (rawIO.readIntLittleEndian(randomAccessFile) == HeaderSignature.END_OF_CENTRAL_DIRECTORY.getValue()) {
                return currentFilePointer;
            }
            numberOfBytesToRead--;
        };

        throw new ZipException("Zip headers not found. Probably not a zip file");
    }

    protected String readZipComment(B2RemoteRandomAccessFile raf, int commentLength, Charset charset) {
        if (commentLength <= 0) {
            return null;
        }

        try {
            byte[] commentBuf = new byte[commentLength];
            raf.readFully(commentBuf);
            return decodeStringWithCharset(commentBuf, false, charset != null ? charset : ZIP4J_DEFAULT_CHARSET);
        } catch (IOException e) {
            // Ignore any exception and set comment to null if comment cannot be read
            return null;
        }
    }

    protected Zip64EndOfCentralDirectoryLocator
    readZip64EndOfCentralDirectoryLocator(B2RemoteRandomAccessFile b2Rraf, RawIO rawIO, long offsetEndOfCentralDirectoryRecord)
            throws IOException {

        Zip64EndOfCentralDirectoryLocator zip64EndOfCentralDirectoryLocator = new Zip64EndOfCentralDirectoryLocator();

        setFilePointerToReadZip64EndCentralDirLoc(b2Rraf, offsetEndOfCentralDirectoryRecord);

        int signature = rawIO.readIntLittleEndian(b2Rraf);
        if (signature == HeaderSignature.ZIP64_END_CENTRAL_DIRECTORY_LOCATOR.getValue()) {
            zipModel.setZip64Format(true);
            zip64EndOfCentralDirectoryLocator.setSignature(HeaderSignature.ZIP64_END_CENTRAL_DIRECTORY_LOCATOR);
        } else {
            zipModel.setZip64Format(false);
            return null;
        }

        zip64EndOfCentralDirectoryLocator.setNumberOfDiskStartOfZip64EndOfCentralDirectoryRecord(
                rawIO.readIntLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryLocator.setOffsetZip64EndOfCentralDirectoryRecord(
                rawIO.readLongLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryLocator.setTotalNumberOfDiscs(rawIO.readIntLittleEndian(b2Rraf));

        return zip64EndOfCentralDirectoryLocator;
    }

    protected void
    setFilePointerToReadZip64EndCentralDirLoc(B2RemoteRandomAccessFile b2Rraf, long offsetEndOfCentralDirectoryRecord)
            throws IOException {

        // Now the file pointer is at the end of signature of Central Dir Rec
        // Seek back with the following values
        // 4 -> total number of disks
        // 8 -> relative offset of the zip64 end of central directory record
        // 4 -> number of the disk with the start of the zip64 end of central directory
        // 4 -> zip64 end of central dir locator signature
        // Refer to Appnote for more information
        seekInCurrentPart(b2Rraf, offsetEndOfCentralDirectoryRecord - 4 - 8 - 4 - 4);
    }

    protected Zip64EndOfCentralDirectoryRecord
    readZip64EndCentralDirRec(B2RemoteRandomAccessFile b2Rraf, RawIO rawIO)
            throws IOException {

        Zip64EndOfCentralDirectoryLocator zip64EndOfCentralDirectoryLocator = zipModel.getZip64EndOfCentralDirectoryLocator();
        if (zip64EndOfCentralDirectoryLocator == null) {
            throw new ZipException("invalid zip64 end of central directory locator");
        }

        long offSetStartOfZip64CentralDir = zip64EndOfCentralDirectoryLocator.getOffsetZip64EndOfCentralDirectoryRecord();

        if (offSetStartOfZip64CentralDir < 0) {
            throw new ZipException("invalid offset for start of end of central directory record");
        }

        b2Rraf.seek(offSetStartOfZip64CentralDir);

        Zip64EndOfCentralDirectoryRecord zip64EndOfCentralDirectoryRecord = new Zip64EndOfCentralDirectoryRecord();

        int signature = rawIO.readIntLittleEndian(b2Rraf);
        if (signature != HeaderSignature.ZIP64_END_CENTRAL_DIRECTORY_RECORD.getValue()) {
            throw new ZipException("invalid signature for zip64 end of central directory record");
        }
        zip64EndOfCentralDirectoryRecord.setSignature(HeaderSignature.ZIP64_END_CENTRAL_DIRECTORY_RECORD);
        zip64EndOfCentralDirectoryRecord.setSizeOfZip64EndCentralDirectoryRecord(rawIO.readLongLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setVersionMadeBy(rawIO.readShortLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setVersionNeededToExtract(rawIO.readShortLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setNumberOfThisDisk(rawIO.readIntLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setNumberOfThisDiskStartOfCentralDirectory(rawIO.readIntLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setTotalNumberOfEntriesInCentralDirectoryOnThisDisk(
                rawIO.readLongLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setTotalNumberOfEntriesInCentralDirectory(rawIO.readLongLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setSizeOfCentralDirectory(rawIO.readLongLittleEndian(b2Rraf));
        zip64EndOfCentralDirectoryRecord.setOffsetStartCentralDirectoryWRTStartDiskNumber(
                rawIO.readLongLittleEndian(b2Rraf));

        //zip64 extensible data sector
        //44 is the size of fixed variables in this record
        long extDataSecSize = zip64EndOfCentralDirectoryRecord.getSizeOfZip64EndCentralDirectoryRecord() - 44;
        if (extDataSecSize > 0) {
            byte[] extDataSecRecBuf = new byte[(int) extDataSecSize];
            b2Rraf.readFully(extDataSecRecBuf);
            zip64EndOfCentralDirectoryRecord.setExtensibleDataSector(extDataSecRecBuf);
        }

        return zip64EndOfCentralDirectoryRecord;
    }

    protected CentralDirectory
    readCentralDirectory(B2RemoteRandomAccessFile b2Rraf, RawIO rawIO, Charset charset)
            throws IOException {

        CentralDirectory centralDirectory = new CentralDirectory();
        List<FileHeader> fileHeaders = new ArrayList<>();

        long offSetStartCentralDir = HeaderUtil.getOffsetStartOfCentralDirectory(zipModel);
        long centralDirEntryCount = getNumberOfEntriesInCentralDirectory(zipModel);

        b2Rraf.seek(offSetStartCentralDir);

        byte[] shortBuff = new byte[2];
        byte[] intBuff = new byte[4];

        for (int i = 0; i < centralDirEntryCount; i++) {
            FileHeader fileHeader = new FileHeader();
            if (rawIO.readIntLittleEndian(b2Rraf) != HeaderSignature.CENTRAL_DIRECTORY.getValue()) {
                throw new ZipException("Expected central directory entry not found (#" + (i + 1) + ")");
            }
            fileHeader.setSignature(HeaderSignature.CENTRAL_DIRECTORY);
            fileHeader.setVersionMadeBy(rawIO.readShortLittleEndian(b2Rraf));
            fileHeader.setVersionNeededToExtract(rawIO.readShortLittleEndian(b2Rraf));

            byte[] generalPurposeFlags = new byte[2];
            b2Rraf.readFully(generalPurposeFlags);
            fileHeader.setEncrypted(isBitSet(generalPurposeFlags[0], 0));
            fileHeader.setDataDescriptorExists(isBitSet(generalPurposeFlags[0], 3));
            fileHeader.setFileNameUTF8Encoded(isBitSet(generalPurposeFlags[1], 3));
            fileHeader.setGeneralPurposeFlag(generalPurposeFlags.clone());

            CompressionMethod compressionMethod = CompressionMethod.getCompressionMethodFromCode(rawIO.readShortLittleEndian(b2Rraf));
            fileHeader.setCompressionMethod(compressionMethod);
            fileHeader.setLastModifiedTime(rawIO.readIntLittleEndian(b2Rraf));

            b2Rraf.readFully(intBuff);
            fileHeader.setCrc(rawIO.readLongLittleEndian(intBuff, 0));

            fileHeader.setCompressedSize(rawIO.readLongLittleEndian(b2Rraf, 4));
            fileHeader.setUncompressedSize(rawIO.readLongLittleEndian(b2Rraf, 4));

            int fileNameLength = rawIO.readShortLittleEndian(b2Rraf);
            fileHeader.setFileNameLength(fileNameLength);

            fileHeader.setExtraFieldLength(rawIO.readShortLittleEndian(b2Rraf));

            int fileCommentLength = rawIO.readShortLittleEndian(b2Rraf);
            fileHeader.setFileCommentLength(fileCommentLength);

            fileHeader.setDiskNumberStart(rawIO.readShortLittleEndian(b2Rraf));

            b2Rraf.readFully(shortBuff);
            fileHeader.setInternalFileAttributes(shortBuff.clone());

            b2Rraf.readFully(intBuff);
            fileHeader.setExternalFileAttributes(intBuff.clone());

            b2Rraf.readFully(intBuff);
            fileHeader.setOffsetLocalHeader(rawIO.readLongLittleEndian(intBuff, 0));

            if (fileNameLength > 0) {
                byte[] fileNameBuff = new byte[fileNameLength];
                b2Rraf.readFully(fileNameBuff);
                String fileName = decodeStringWithCharset(fileNameBuff, fileHeader.isFileNameUTF8Encoded(), charset);
                fileHeader.setFileName(fileName);
            } else {
                throw new ZipException("Invalid entry name in file header");
            }

            fileHeader.setDirectory(isDirectory(fileHeader.getExternalFileAttributes(), fileHeader.getFileName()));
            readExtraDataRecords(b2Rraf, fileHeader);
            readZip64ExtendedInfo(fileHeader, rawIO);
            readAesExtraDataRecord(fileHeader, rawIO);

            if (fileCommentLength > 0) {
                byte[] fileCommentBuff = new byte[fileCommentLength];
                b2Rraf.readFully(fileCommentBuff);
                fileHeader.setFileComment(decodeStringWithCharset(fileCommentBuff, fileHeader.isFileNameUTF8Encoded(), charset));
            }

            if (fileHeader.isEncrypted()) {
                if (fileHeader.getAesExtraDataRecord() != null) {
                    fileHeader.setEncryptionMethod(EncryptionMethod.AES);
                } else {
                    fileHeader.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
                }
            }

            fileHeaders.add(fileHeader);
        }

        centralDirectory.setFileHeaders(fileHeaders);

        DigitalSignature digitalSignature = new DigitalSignature();
        if (rawIO.readIntLittleEndian(b2Rraf) == HeaderSignature.DIGITAL_SIGNATURE.getValue()) {
            digitalSignature.setSignature(HeaderSignature.DIGITAL_SIGNATURE);
            digitalSignature.setSizeOfData(rawIO.readShortLittleEndian(b2Rraf));

            if (digitalSignature.getSizeOfData() > 0) {
                byte[] signatureDataBuff = new byte[digitalSignature.getSizeOfData()];
                b2Rraf.readFully(signatureDataBuff);
                digitalSignature.setSignatureData(new String(signatureDataBuff));
            }
        }

        return centralDirectory;
    }

    protected void
    readExtraDataRecords(B2RemoteRandomAccessFile b2Rraf, FileHeader fileHeader)
            throws IOException {

        int extraFieldLength = fileHeader.getExtraFieldLength();
        if (extraFieldLength <= 0) {
            return;
        }

        fileHeader.setExtraDataRecords(readExtraDataRecords(b2Rraf, extraFieldLength));
    }

    protected List<ExtraDataRecord>
    readExtraDataRecords(B2RemoteRandomAccessFile b2Rraf, int extraFieldLength)
            throws IOException {

        if (extraFieldLength < 4) {
            if (extraFieldLength > 0) {
                b2Rraf.skipBytes(extraFieldLength);
            }

            return null;
        }

        byte[] extraFieldBuf = new byte[extraFieldLength];
        b2Rraf.read(extraFieldBuf);

        try {
            return parseExtraDataRecords(extraFieldBuf, extraFieldLength);
        } catch (Exception e) {
            // Ignore any errors when parsing extra data records
            return Collections.emptyList();
        }
    }

}
