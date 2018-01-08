package com.blackboard.safeassign;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.forName;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.filefilter.FileFilterUtils.falseFileFilter;
import static org.apache.commons.io.filefilter.FileFilterUtils.suffixFileFilter;
import static org.apache.commons.lang3.BooleanUtils.toBoolean;

/**
 * Two S3 tests based on S3Client and TransferManager and one (E)FS test based on Apache IO utils
 *
 * Created by srakonjac on 1/8/18.
 */
public class S3VSEFSTest implements Runnable {

  private final ConsoleLogger LOGGER = new ConsoleLogger();

  private static final String REGION = "us-east-1";
  private static final String ENDPOINT = "https://s3.amazonaws.com";
  private static final String BUCKET_NAME = "s3vsefstestbucket";
  private static final String MOUNT_POINT_DIR_NAME = "/efs";

  private final boolean debug;

  private final AmazonS3 s3Client;
  private final TransferManager transferManager;

  public static void main(String ... args) {
    Validate.isTrue(args.length == 2 || args.length == 3, "Expected two or three arguments: AWS access-key and secret-key (resp. 'debug' flag)");
    new S3VSEFSTest(args[0], args[1], args.length == 3 ? toBoolean(args[2]) : false).run();
  }

  private S3VSEFSTest(final String accessKey, final String secretKey, boolean debug) {

    this.debug = debug;

    AWSCredentialsProvider credentialsProvider = new AWSCredentialsProvider() {

      @Override
      public AWSCredentials getCredentials() {
        return new BasicAWSCredentials(accessKey, secretKey);
      }

      @Override
      public void refresh() { }
    };

    s3Client = AmazonS3ClientBuilder.standard()
                                    .withCredentials(credentialsProvider)
                                    .withEndpointConfiguration(new EndpointConfiguration(ENDPOINT, REGION))
                                    .build();

    transferManager = TransferManagerBuilder.standard()
                                            .withS3Client(s3Client)
                                            .build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {

    LOGGER.info("Starting {} tests", getClass().getSimpleName());

    for(Object[] data : dataProvider()) {

      try {

        serialFS((String) data[0], (Collection<File>) data[1]);

        serialS3((String) data[0], (Collection<File>) data[1]);

        serialS3TransferManagered((String) data[0], (Collection<File>) data[1]);

      } catch (Exception ex) {
        LOGGER.error("Failed data [{}, <collection>] with error message: {}", data[0], ex.getMessage(), ex);
      }
    }

    LOGGER.info("Ended {} tests", getClass().getSimpleName());
  }

  public Object[][] dataProvider() {
    return new Object[][] {
      {
        "75-files mixed-batch",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/75-mixed").getPath()), suffixFileFilter("txt"), falseFileFilter())
      },
      {
        "150-files mixed-batch",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/150-mixed").getPath()), suffixFileFilter("txt"), falseFileFilter())
      },
      {
        "300-files mixed-batch",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/300-mixed").getPath()), suffixFileFilter("txt"), falseFileFilter())
      },
      {
        "100-files 5-paragraph",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/100x-5-paragraph").getPath()), suffixFileFilter("txt"), falseFileFilter())
      },
      {
        "100-files 10-paragraph",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/100x-10-paragraph").getPath()), suffixFileFilter("txt"), falseFileFilter())
      },
      {
        "100-files 20-paragraph",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/100x-20-paragraph").getPath()), suffixFileFilter("txt"), falseFileFilter())
      },
      {
        "100-files 50-paragraph",
        listFiles(new File(getClass().getClassLoader().getResource("./batches/100x-50-paragraph").getPath()), suffixFileFilter("txt"), falseFileFilter())
      }
    };
  }

  private void serialS3(String descriptor, Collection<File> files) throws IOException {

    LOGGER.info("Running Serial S3 Test: {}", descriptor);

    final Long startSerial = currentTimeMillis();

    for (File f : files) {

      try(InputStream is = new FileInputStream(f)) {

        final String filePath = "test-serial" + "/" + f.getParentFile().getName() + "/" + f.getName();

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("originalLength", valueOf(is.available()));

        Long start = currentTimeMillis();
        s3Client.putObject(new PutObjectRequest(BUCKET_NAME, filePath, is, metadata));
        LOGGER.debug("Writing {} to S3 took {}ms", filePath, currentTimeMillis() - start);
      }
    }

    LOGGER.info("Serial S3 Test {} took {}ms", descriptor, currentTimeMillis() - startSerial);
  }

  private void serialS3TransferManagered(String descriptor, Collection<File> files) throws IOException, InterruptedException {

    LOGGER.info("Running Serial S3 TransferManager-ed Test: {}", descriptor);

    final Long startSerial = currentTimeMillis();

    for (File f : files) {

      final String filePath = "test-serial-tm"
                            + "/" + f.getParentFile().getName()
                            + "/" + f.getName();

      Long start = currentTimeMillis();
      Upload upload = transferManager.upload(BUCKET_NAME, filePath, f);
      upload.waitForUploadResult();
      LOGGER.debug("Writing {} to S3 took {}ms", filePath, currentTimeMillis() - start);
    }

    LOGGER.info("Serial S3 TransferManager-ed Test {} took {}ms", descriptor, currentTimeMillis() - startSerial);
  }

  private void serialFS(String descriptor, Collection<File> files) throws IOException {

    LOGGER.info("Running Serial EFS Test: {}", descriptor);

    final Long startSerial = currentTimeMillis();

    for (File f : files) {

      final String parentPath = MOUNT_POINT_DIR_NAME
                              + "/" + "test-serial"
                              + "/" + f.getParentFile().getName();
      final File parent = new File(parentPath);
      if(!parent.exists()) {
        parent.mkdirs();
      }

      final String filePath = parentPath + "/" + f.getName();

      Long start = currentTimeMillis();
      IOUtils.write(readFileToString(f, forName("UTF-8")), new FileWriter(filePath));
      LOGGER.debug("Writing {} to EFS took {}ms", filePath, currentTimeMillis() - start);
    }

    LOGGER.info("Serial EFS Test {} took {}ms", descriptor, currentTimeMillis() - startSerial);
  }

  private class ConsoleLogger {
    void debug(String pattern, Object ... args) {
      if(debug) {
        info(pattern, args);
      }
    }
    void error(String pattern, Object ... args) {
      info(pattern, args);
    }
    void info(String pattern, Object ... args) {
      System.out.println(format(pattern.replace("{}", "%s"), args));
    }
  }
}
