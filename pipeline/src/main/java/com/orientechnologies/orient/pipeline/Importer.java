package com.orientechnologies.orient.pipeline;

import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Importer {
  private static final int numberOfIterations = 3;
  private static final String databaseName = "testBench";

  public static void main(String[] args) {
    final Importer importer = new Importer();
    final List<String> paths =
        Arrays.asList(
            "databases_2_2\\Empty.json",
            "databases_2_2\\OrderCustomer-sl-0.json",
            "databases_3_1\\OrderCustomer-sl-0.json");
    importer.execute(paths, numberOfIterations);
  }

  public void execute(final List<String> paths, final int numberOfIterations) {
    try (final ProgressBar pb = new ProgressBar("Iterations", numberOfIterations)) {
      final List<Workload> workloads = new ArrayList<>();
      for (final String path : paths) {
        final SummaryStatistics importStats = new SummaryStatistics();
        final SummaryStatistics exportStats = new SummaryStatistics();
        for (int i = 0; i < numberOfIterations; i++) {
          executeWorkload(path, importStats, exportStats);
          Thread.sleep(2000);
          pb.step();
        }
        pb.stepTo(numberOfIterations);

        System.out.println();
        System.out.println("+++++++++++++++++");
        System.out.println(
            "\t"
                + "Import"
                + "(ms): "
                + importStats.getMean()
                + ", error="
                + calculateMeanCI(importStats, CI_LEVEL));
        System.out.println(
            "\t"
                + "Export"
                + "(ms): "
                + exportStats.getMean()
                + ", error="
                + calculateMeanCI(importStats, CI_LEVEL));
        System.out.println("+++++++++++++++++");
      }
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void executeWorkload(
      final String path, final SummaryStatistics importStats, final SummaryStatistics exportStats) {
    final Operation importOperation = new ImportExportOperation("EmptyImport", databaseName);
    InputStream is = Importer.class.getClassLoader().getResourceAsStream(path);
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    importOperation.setup(is, output);
    long start = System.nanoTime();
    importOperation.executeImport();
    long elapsed = (System.nanoTime() - start) / 1000000;
    importStats.addValue(elapsed);
    System.out.println("Elapsed(import)=" + elapsed);

    start = System.nanoTime();
    importOperation.executeExport();
    elapsed = (System.nanoTime() - start) / 1000000;
    exportStats.addValue(elapsed);
    System.out.println("Elapsed(export)=" + elapsed);
    System.out.println("Size(output)" + output.toByteArray().length);
    importOperation.tearDown();
  }

  private static final double CI_LEVEL = 0.95;

  private double calculateMeanCI(final SummaryStatistics stats, double level) {
    try {
      final TDistribution tDist = new TDistribution(stats.getN() - 1);
      final double criticalValue = tDist.inverseCumulativeProbability(1.0 - (1 - level) / 2);
      return criticalValue * stats.getStandardDeviation() / Math.sqrt(stats.getN());
    } catch (final Exception e) {
      System.out.println("Failed to calculate the mean CI" + e.getMessage());
      return Double.NaN;
    }
  }
}
