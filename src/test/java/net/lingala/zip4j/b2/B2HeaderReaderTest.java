package net.lingala.zip4j.b2;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;
import net.lingala.zip4j.AbstractIT;
import net.lingala.zip4j.headers.B2HeaderReader;
import net.lingala.zip4j.headers.HeaderReader;
import net.lingala.zip4j.io.inputstream.B2File;
import net.lingala.zip4j.io.inputstream.B2RemoteRandomAccessFile;
import net.lingala.zip4j.model.CentralDirectory;
import net.lingala.zip4j.model.EndOfCentralDirectoryRecord;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.ZipModel;
import net.lingala.zip4j.util.InternalZipConstants;
import org.junit.Test;

import java.sql.Timestamp;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static net.lingala.zip4j.testutils.TestUtils.hexDump;
import static org.assertj.core.api.Assertions.assertThat;


public class B2HeaderReaderTest extends AbstractIT {
    protected PrintWriter writer = new PrintWriter(System.err, true);

    protected static final String APPLICATION_KEY_ID = "001440aed7310bd0000000006";
    protected static final String APPLICATION_KEY = "K001107NDDlaukcpSDebx8awJGP/8aY";
    protected static final String USER_AGENT = "EnhancedZip4J";

    @Test
    public void testReadHeaders() throws Exception {
        // throws RunTimeException when client can't be created
        B2StorageClient client = B2StorageClientFactory
                .createDefaultFactory()
                .create(APPLICATION_KEY_ID, APPLICATION_KEY, USER_AGENT);
        assertThat(client).isNotNull();

        // throws B2Exception
        List<B2Bucket> buckets = client.buckets();
        assertThat(buckets).isNotNull();
        assertThat(buckets.size()).isNotZero();

        // throws B2Exception
        B2Bucket bucket = buckets.get(0);
        B2ListFilesIterable files = client.fileNames(bucket.getBucketId());
        assertThat(files).isNotNull();
        Iterator<B2FileVersion> filesIterator = files.iterator();
        assertThat(filesIterator.hasNext()).isTrue();

        B2FileVersion file = filesIterator.next();
        assertThat(file).isNotNull();

        B2File b2File = new B2File(bucket, file);
        assertThat(b2File).isNotNull();
        B2RemoteRandomAccessFile b2RemoteFile = new B2RemoteRandomAccessFile(b2File);
        assertThat(b2RemoteFile).isNotNull();
        B2HeaderReader reader = new B2HeaderReader();
        assertThat(reader).isNotNull();
        ZipModel model = reader.readAllHeaders(b2RemoteFile, buildDefaultConfig());
        assertThat(model).isNotNull();

        boolean isZip64 = model.isZip64Format();
        assertThat(isZip64).isTrue();

        EndOfCentralDirectoryRecord endOfCentralDirectoryRecord = model.getEndOfCentralDirectoryRecord();
        assertThat(endOfCentralDirectoryRecord).isNotNull();
        int directoryEntries = endOfCentralDirectoryRecord.getTotalNumberOfEntriesInCentralDirectory();
        writer.println(NumberFormat.getInstance().format(directoryEntries) + " directory entries");

        CentralDirectory centralDirectory = model.getCentralDirectory();
        assertThat(centralDirectory).isNotNull();
        List<FileHeader> fileHeaders = centralDirectory.getFileHeaders();
        assertThat(fileHeaders).isNotNull();
        assertThat(fileHeaders.size()).isGreaterThan(0);
        writer.println(NumberFormat.getInstance().format(fileHeaders.size()) + " file headers");

//        for (FileHeader header : fileHeaders) {
//            writer.println("name: " + header.getFileName()
//                    + (header.isEncrypted() ? " encrypted" : " cleartext")
//                    + (header.isDirectory() ? " directory" : " file")
//                    + " last modified: " + (new Timestamp(header.getLastModifiedTime()))
//                    + " compressed size: " + NumberFormat.getInstance().format(header.getCompressedSize()) + " bytes"
//                    + " uncompressed size: " + NumberFormat.getInstance().format(header.getUncompressedSize()) + " bytes"
//            );
//        }

//        fileHeaders.stream()
//                .filter(header -> header.getFileName().contains(".photoslibrary"))
//                .forEach(header -> writer.println("name: " + header.getFileName()
//                    + (header.isEncrypted() ? " encrypted" : " cleartext")
//                    + (header.isDirectory() ? " directory" : " file")
//                    + " last modified: " + (new Timestamp(header.getLastModifiedTime()))
//                    + " compressed size: " + NumberFormat.getInstance().format(header.getCompressedSize()) + " bytes"
//                    + " uncompressed size: " + NumberFormat.getInstance().format(header.getUncompressedSize()) + " bytes"
//                    )
//                );
    }

}
