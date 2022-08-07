package net.lingala.zip4j;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;

import net.lingala.zip4j.io.inputstream.B2File;
import net.lingala.zip4j.model.FileHeader;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class B2ExtractZipFileTest extends AbstractIT {
    protected PrintWriter writer = new PrintWriter(System.err, true);

    protected static final String APPLICATION_KEY_ID = "001440aed7310bd0000000006";
    protected static final String APPLICATION_KEY = "K001107NDDlaukcpSDebx8awJGP/8aY";
    protected static final String USER_AGENT = "EnhancedZip4J";

    @Test
    public void testExtractOneFileToFileSystem() throws Exception {
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

        ZipFile zipFile = new ZipFile(b2File);
        assertThat(zipFile).isNotNull();
        String pathToExtract = "Macintosh HD/Users/bella/Pictures/Photos Library.photoslibrary/";
        String destinationPath = "/Users/bluepill/Temp/";
        zipFile.extractFile(pathToExtract, destinationPath);

//        List<FileHeader> headers = zipFile.getFileHeaders();
//        assertThat(headers).isNotNull();
//        HashMap<String, FileHeader> failedExistenceCheckHeaders = new HashMap<>(64);
//        HashMap<String, FileHeader> succeededExistenceCheckHeaders = new HashMap<>(5128);
//        HashMap<String, Pair<FileHeader, Exception>> exceptionedHeaders = new HashMap<>(32);
//        headers.forEach( header -> {
//            assertThat(header).isNotNull();
//            if (header.getFileName().startsWith(pathToExtract)) {
//                writer.print(" [" + NumberFormat.getInstance().format(header.getUncompressedSize()) + "] " + header.getFileName() );
//                try { zipFile.extractFile(header, destinationPath);
//                } catch (Exception e) {
//                    writer.println(" EXCEPTIONED!");
//                    exceptionedHeaders.put(destinationPath + header.getFileName(), new Pair<>(header, e) );
//                }
//                writer.println(" extracted to " + destinationPath + header.getFileName());
//
//                File extractedFile = new File(destinationPath, header.getFileName());
//                if (extractedFile.exists()) { succeededExistenceCheckHeaders.put(destinationPath + header.getFileName(), header); }
//                else { failedExistenceCheckHeaders.put(destinationPath + header.getFileName(), header); }
//            }
//        });
//        writer.println("\ttotal succeeded = " + NumberFormat.getInstance().format(succeededExistenceCheckHeaders.size()));
//        writer.println("\ttotal failed = " + NumberFormat.getInstance().format(failedExistenceCheckHeaders.size()));
//        writer.println("\ttotal exceptioned = " + NumberFormat.getInstance().format(exceptionedHeaders.size()));
//        failedExistenceCheckHeaders.forEach( (k, v) -> { writer.println("failed: " + k); });
//        writer.println("\ttotal succeeded = " + NumberFormat.getInstance().format(succeededExistenceCheckHeaders.size()));
//        writer.println("\ttotal failed = " + NumberFormat.getInstance().format(failedExistenceCheckHeaders.size()));
//        writer.println("\ttotal exceptioned = " + NumberFormat.getInstance().format(exceptionedHeaders.size()));
//        exceptionedHeaders.forEach( (k, v) -> { writer.println("exceptioned: " + k); });
//        writer.println("\ttotal succeeded = " + NumberFormat.getInstance().format(succeededExistenceCheckHeaders.size()));
//        writer.println("\ttotal failed = " + NumberFormat.getInstance().format(failedExistenceCheckHeaders.size()));
//        writer.println("\ttotal exceptioned = " + NumberFormat.getInstance().format(exceptionedHeaders.size()));
;    }

}
