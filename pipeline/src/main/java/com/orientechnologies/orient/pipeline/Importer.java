package com.orientechnologies.orient.pipeline;

import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.*;

public class Importer {
  private static final int numberOfIterations = 25;
  private static final String databaseName = "testBench";

  public static void main(String[] args) {
    try (final ProgressBar pb = new ProgressBar("Iterations", numberOfIterations)) {
      final SummaryStatistics statistics = new SummaryStatistics();
      for (int i = 0; i < numberOfIterations; i++) {
        final Operation importOperation = new ImportOperation("EmptyImport", databaseName);
        InputStream is =
            Importer.class
                .getClassLoader()
                .getResourceAsStream("databases_2_2\\OrderCustomer-sl-0.json");
        // importOperation.setup("databases_3_1\\OrderCustomer-sl-0");
        importOperation.setup(is);
        long start = System.nanoTime();
        importOperation.execute();
        final long elapsed = (System.nanoTime() - start) / 1000000;
        statistics.addValue(elapsed);
        System.out.println("Elapsed=" + elapsed);
        importOperation.tearDown();
        Thread.sleep(2000);
        pb.step();
      }
      pb.stepTo(numberOfIterations);
      System.out.println(
          "\t"
              + "Test"
              + "(ms): "
              + statistics.getMean()
              + ", error="
              + calculateMeanCI(statistics, CI_LEVEL));
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }

    /*final ByteArrayOutputStream output = new ByteArrayOutputStream();
    //try {
      //final InputStream is =
      //    new URL("https://orientdb.com/public-databases/3.0.x/MovieRatings.zip").openStream();
      //
      // IOUtils.copy(is, output);
      //try (final ZipInputStream zis = new ZipInputStream(is)) {
      //  zis.getNextEntry();
      //  byte[] buf = new byte[1024];
      //  int len = zis.read(buf);
      //  while (len > 0) {
      //    output.write(buf, 0, len);
      //    len = zis.read(buf);
      //  }
      //}
    //} catch (final IOException e) {
    //  e.printStackTrace();
    //}
    final Workload workload = getImportWorkload(output.toByteArray());
    final Importer importer = new Importer();
    importer.execute(workload, numberOfIterations);*/
  }

  private static final double CI_LEVEL = 0.95;

  private static double calculateMeanCI(final SummaryStatistics stats, double level) {
    try {
      final TDistribution tDist = new TDistribution(stats.getN() - 1);
      final double criticalValue = tDist.inverseCumulativeProbability(1.0 - (1 - level) / 2);
      return criticalValue * stats.getStandardDeviation() / Math.sqrt(stats.getN());
    } catch (final Exception e) {
      System.out.println("Failed to calculate the mean CI" + e.getMessage());
      return Double.NaN;
    }
  }

  private static Workload getImportWorkload(final byte[] bytes) {
    final Operation importOperation = new ImportOperation("EmptyImport", databaseName, bytes);
    return new Workload("Import", importOperation);
  }

  private static Workload getEmptyExportImportWorkload() {
    final Operation exportOperation = new ExportOperation("EmptyExport", databaseName);

    final Operation importOperation = new ImportOperation("EmptyImport", databaseName);
    return new Workload("Empty Import/Export", exportOperation, importOperation);
  }

  private void execute(final Workload workload, final int numberOfIterations) {
    try (final ProgressBar pb = new ProgressBar("Iterations", numberOfIterations)) {
      // workload.setup();
      for (int i = 0; i < numberOfIterations; i++) {
        executeWorkload(workload);
        pb.step();
      }
      pb.stepTo(numberOfIterations);
    } catch (IOException e) {
      e.printStackTrace();
    }
    workload.printStats();
  }

  private void executeWorkload(final Workload workload) throws IOException {
    workload.setup();
    workload.execute();
    workload.tearDown();
  }
}
