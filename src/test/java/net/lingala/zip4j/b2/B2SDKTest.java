package net.lingala.zip4j.b2;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;

import com.backblaze.b2.client.contentHandlers.B2ContentFileWriter;
import com.backblaze.b2.client.contentHandlers.B2ContentMemoryWriter;
import com.backblaze.b2.client.structures.*;
import com.backblaze.b2.util.B2ByteRange;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import java.io.File;
import java.io.PrintWriter;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import com.backblaze.b2.client.exceptions.B2Exception;

import static net.lingala.zip4j.testutils.TestUtils.hexDump;
import static net.lingala.zip4j.util.InternalZipConstants.MIN_BUFF_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;


public class B2SDKTest {

    // using: EnhancedZip4J application key
    protected static final String APPLICATION_KEY_ID = "001440aed7310bd0000000006";
    protected static final String APPLICATION_KEY = "K001107NDDlaukcpSDebx8awJGP/8aY";
    protected static final String USER_AGENT = "EnhancedZip4J";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateStorageClientThrowsExceptionWhenNoImplementation() throws Exception {
        // throws RunTimeException when client can't be created
        B2StorageClient client = B2StorageClientFactory
                .createDefaultFactory()
                .create(APPLICATION_KEY_ID, APPLICATION_KEY, USER_AGENT);

        assertThat(client).isNotNull();
    }

    @Test
    public void testListBucketsThrowsExceptionWithBadCredentials() throws B2Exception {
        expectedException.reportMissingExceptionWithMessage("Missing Unauthorised exception");
        expectedException.expect(B2Exception.class);

        // throws RunTimeException when client can't be created
        B2StorageClient client = B2StorageClientFactory
                .createDefaultFactory()
                .create("", "", USER_AGENT);
        assertThat(client).isNotNull();

        List<B2Bucket> buckets = client.buckets();
    }

    @Test
    public void testListBucketsHasContent() throws B2Exception {
        // throws RunTimeException when client can't be created
        B2StorageClient client = B2StorageClientFactory
                .createDefaultFactory()
                .create(APPLICATION_KEY_ID, APPLICATION_KEY, USER_AGENT);
        assertThat(client).isNotNull();

        // throws B2Exception
        List<B2Bucket> buckets = client.buckets();
        assertThat(buckets).isNotNull();
        assertThat(buckets.size()).isNotZero();

        PrintWriter writer = new PrintWriter(System.out, true);
        for (B2Bucket bucket : buckets) {
            writer.println(" " + bucket);
        }
    }

    @Test
    public void testListFilesHasContent() throws B2Exception {
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
        B2ListFilesIterable files = client.fileNames(buckets.get(0).getBucketId());
        assertThat(files).isNotNull();
        Iterator<B2FileVersion> filesIterator = files.iterator();
        assertThat(filesIterator.hasNext()).isTrue();

        PrintWriter writer = new PrintWriter(System.out, true);
        for (B2FileVersion file : files) {
            writer.println(" " + file);
        }
    }

    @Test
    public void testDownloadRangeHasContent() throws B2Exception {
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
        B2ListFilesIterable files = client.fileNames(buckets.get(0).getBucketId());
        assertThat(files).isNotNull();
        Iterator<B2FileVersion> filesIterator = files.iterator();
        assertThat(filesIterator.hasNext()).isTrue();

        B2FileVersion file = filesIterator.next();
        assertThat(file).isNotNull();

        B2DownloadByIdRequest request = B2DownloadByIdRequest
                .builder(file.getFileId())
                .setRange(B2ByteRange.startAt( file.getContentLength() - 1024) )
                .build();
        B2ContentMemoryWriter sink = B2ContentMemoryWriter.build();
        client.downloadById(request, sink);
        byte[] bytes = sink.getBytes();
        assertThat(bytes).isNotNull();
        assertThat(bytes.length).isNotZero();

        PrintWriter writer = new PrintWriter(System.out, true);
        hexDump(0, writer, bytes, 0, bytes.length);
    }
}
