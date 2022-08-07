package net.lingala.zip4j.testutils;

import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.FileUtils;
import net.lingala.zip4j.util.InternalZipConstants;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class TestUtils {

  private static final String TEST_FILES_FOLDER_NAME = "test-files";
  private static final String TEST_ARCHIVES_FOLDER_NAME = "test-archives";

  public final static String NULL = "null";
  /** A table of hex digits */
  private static final char[] hexDigit = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
          'E', 'F' };
  private static BitSet printChars = null;
  private static final String PRINTABLE = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

  public static File getTestFileFromResources(String fileName) {
    return getFileFromResources(TEST_FILES_FOLDER_NAME, fileName);
  }

  public static File getTestArchiveFromResources(String fileName) {
   return getFileFromResources(TEST_ARCHIVES_FOLDER_NAME, fileName);
  }

  /**
   * Splits files with extension .001, .002, etc
   * @param fileToSplit file to be split
   * @param splitLength the length of each split file
   * @return File - first split file
   * @throws IOException if any exception occurs dealing with streams
   */
  public static File splitFileWith7ZipFormat(File fileToSplit, File outputFolder, long splitLength) throws IOException {
    if (splitLength < InternalZipConstants.MIN_SPLIT_LENGTH) {
      throw new IllegalArgumentException("split length less than minimum allowed split length of " + InternalZipConstants.MIN_SPLIT_LENGTH);
    }

    int splitCounter = 0;
    byte[] buff = new byte[InternalZipConstants.BUFF_SIZE];
    int readLen = 0;
    long numberOfBytesWrittenInThisPart = 0;

    try (InputStream inputStream = new FileInputStream(fileToSplit)) {
      OutputStream outputStream = startNext7ZipSplitStream(fileToSplit, outputFolder, splitCounter);
      splitCounter++;

      while ((readLen = inputStream.read(buff)) != -1) {
        if (numberOfBytesWrittenInThisPart + readLen > splitLength) {
          int numberOfBytesToWriteInThisCounter = (int) (splitLength - numberOfBytesWrittenInThisPart);
          outputStream.write(buff, 0, numberOfBytesToWriteInThisCounter);
          outputStream.close();
          outputStream = startNext7ZipSplitStream(fileToSplit, outputFolder, splitCounter);
          splitCounter++;
          outputStream.write(buff, numberOfBytesToWriteInThisCounter, readLen - numberOfBytesToWriteInThisCounter);
          numberOfBytesWrittenInThisPart = readLen - numberOfBytesToWriteInThisCounter;
        } else {
          outputStream.write(buff, 0, readLen);
          numberOfBytesWrittenInThisPart += readLen;
        }
      }

      outputStream.close();
    }

    return getFileNameFor7ZipSplitIndex(fileToSplit, outputFolder, 0);
  }

  public static void copyFile(File sourceFile, File destinationFile) throws IOException {
    Files.copy(sourceFile.toPath(), destinationFile.toPath());
  }

  public static void copyFileToFolder(File sourceFile, File outputFolder) throws IOException {
    File destinationFile = new File(outputFolder.getAbsolutePath(), sourceFile.getName());
    copyFile(sourceFile, destinationFile);
  }

  public static void copyFileToFolder(File sourceFile, File outputFolder, int numberOfCopiesToMake) throws IOException {
    for (int i = 0; i < numberOfCopiesToMake; i++) {
      File destinationFile = new File(outputFolder.getAbsolutePath(), i + ".pdf");
      copyFile(sourceFile, destinationFile);
    }
  }

  public static void copyDirectory(File sourceDirectory, File destinationDirectory) throws IOException {
    if (!destinationDirectory.exists()) {
      destinationDirectory.mkdir();
    }
    for (String f : sourceDirectory.list()) {
      copyDirectoryCompatibilityMode(new File(sourceDirectory, f), new File(destinationDirectory, f));
    }
  }

  public static void copyDirectoryCompatibilityMode(File source, File destination) throws IOException {
    if (source.isDirectory()) {
      copyDirectory(source, destination);
    } else {
      copyFile(source, destination);
    }
  }

  public static void createZipFileWithZipOutputStream(File zipFile, List<File> filesToAdd) throws IOException {

    byte[] buff = new byte[InternalZipConstants.BUFF_SIZE];
    int readLen = -1;
    ZipParameters zipParameters = new ZipParameters();

    try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile))) {
      for (File fileToAdd : filesToAdd) {
        zipParameters.setFileNameInZip(fileToAdd.getName());
        zipOutputStream.putNextEntry(zipParameters);

        try(InputStream inputStream = new FileInputStream(fileToAdd)) {
          while ((readLen = inputStream.read(buff)) != -1) {
            zipOutputStream.write(buff, 0, readLen);
          }
        }

        zipOutputStream.closeEntry();
      }
    }
  }

  public static File generateFileOfSize(TemporaryFolder temporaryFolder, long fileSize) throws IOException {
    File outputFile = temporaryFolder.newFile();
    byte[] b = new byte[8 * InternalZipConstants.BUFF_SIZE];
    Random random = new Random();
    long bytesWritten = 0;
    int bufferWriteLength;

    try (OutputStream outputStream = new FileOutputStream(outputFile)) {
      while (bytesWritten < fileSize) {
        random.nextBytes(b);
        bufferWriteLength = bytesWritten + b.length > fileSize ? ((int) (fileSize - bytesWritten)) : b.length;
        outputStream.write(b, 0, bufferWriteLength);
        bytesWritten += bufferWriteLength;
      }
    }

    return outputFile;
  }

  public static File createSymlink(File targetFile, File rootFolder) throws IOException {
    Path link = Paths.get(rootFolder.getAbsolutePath(), "symlink.link");
    Files.createSymbolicLink(link, targetFile.toPath());
    return link.toFile();
  }

  public static List<String> getFileNamesOfFiles(List<File> files) {
    List<String> fileNames = new ArrayList<>();
    if (files.isEmpty()) {
      return fileNames;
    }

    for (File file : files) {
      fileNames.add(file.getName());
    }
    return fileNames;
  }

  private static OutputStream startNext7ZipSplitStream(File sourceFile, File outputFolder, int index) throws IOException {
    File outputFile = getFileNameFor7ZipSplitIndex(sourceFile, outputFolder, index);
    return new FileOutputStream(outputFile);
  }

  private static File getFileNameFor7ZipSplitIndex(File sourceFile, File outputFolder, int index) throws IOException {
    return new File(outputFolder.getCanonicalPath() + File.separator + sourceFile.getName()
        + FileUtils.getNextNumberedSplitFileCounterAsExtension(index));
  }

  private static File getFileFromResources(String parentFolder, String fileName) {
    try {
      String path = "/" + parentFolder + "/" + fileName;
      URL fileUrl = TestUtils.class.getResource(path);
      if (fileUrl == null) {
        throw new RuntimeException("File not found " + path);
      }
      String utfDecodedFilePath = URLDecoder.decode(fileUrl.getFile(), InternalZipConstants.CHARSET_UTF_8.toString());
      return new File(utfDecodedFilePath);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Initialize bitset for isPrintable()
   */
  private static synchronized void initPrintChars() {
    if (printChars == null) {
      printChars = new BitSet(128);
      for (int i = 0; i < PRINTABLE.length(); i++) {
        printChars.set(PRINTABLE.charAt(i));
      }
    }
  }

  /**
   * Returns <code>true</code> if characters may be considered "printable"
   */
  public static boolean isPrintable(char ch) {
    if (!Character.isISOControl(ch)) {
      if (printChars == null)
        initPrintChars();
      return printChars.get(ch);
    }
    return false;
  }

  /**
   * Converts a nibble to a hex character
   * @param   nibble   the nibble to convert.
   * @return the hex character
   */
  private static char toHex(int nibble) {
    return hexDigit[(nibble & 0xF)];
  }

  /**
   * Dumps binary data, presenting raw bytes and their character equivalents.
   * The output looks like this:
   * <pre>
   * 00000000  01 04 04 00 7F E7 55 03 6F 76 65 72 76 69 65 77  ......U.overview
   * 00000010  00 36 03 4F 76 65 72 76 69 65 77 00 01 60 64 03  .6.Overview..`d.
   * 00000020  57 68 61 74 20 69 73 20 57 41 50 3F 00 01 26 26  What is WAP?..&&
   * </pre>
   *
   * @param start start "offset" (the left column)
   * @param w consumes the output
   * @param data the data to be displayed
   * @param off the start offset in the data array
   * @param len number of bytes to dump.
   */
  public static void hexDump(int start, PrintWriter w, byte[] data, int off, int len) {
    if (data != null || len != 0) {
      int rowSize = 16; // Number of bytes to show in a row
      int addrBits = (((off + len) < 0x10000) ? 16 : ((off + len) < 0x1000000 ? 24 : 32));

      int nRows = (len + rowSize - 1) / rowSize;
      for (int row = 0; row < nRows; row++) {

        // Create row address label:
        int addr = start + row * rowSize;
        for (int i = addrBits - 4; i >= 0; i -= 4)
          w.print(toHex(addr >> i));
        w.print(" ");

        // Show the bytes plus their renderable characters:
        for (int offset = 0; offset < rowSize; offset++) {
          int index = (row * rowSize) + offset;
          if (index < len) {
            // Separate bytes by a single space
            w.print(" ");
            int b = data[off + index];
            w.print(toHex(b >> 4)); // upper nibble
            w.print(toHex(b)); // lower nibblebble
          } else {
            // Pad partial line with spaces
            // so that the character version will align correctly:
            w.print("   ");
          }
        }
        // Add character version of row data:
        w.print("  ");
        for (int offset = 0; offset < rowSize; offset++) {
          int index = (row * rowSize) + offset;
          if (index < data.length) {
            char ch = (char) data[off + index];
            if (isPrintable(ch)) {
              w.print(ch); // displayable character
            } else {
              w.print('.');
            }
          } else {
            // Pad partial line with spaces
            // so that all lines have equal length
            w.print(' ');
          }
        }
        w.println();
      }
    }
  }

}
