package com.orientechnologies.orient.pipeline;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Workload {
  private static final double CI_LEVEL = 0.95;

  public String getName() {
    return name;
  }

  private final String name;

  public List<Operation> getOperations() {
    return operations;
  }

  private final List<Operation> operations;
  private final Map<String, SummaryStatistics> stats = new HashMap<>();

  public Workload(final String name, final Operation firstOperation, final Operation secondOperation) {
    this.name = name;
    this.operations = Arrays.asList(firstOperation, secondOperation);

    for (final Operation operation : operations) {
      stats.put(operation.getName(), new SummaryStatistics());
    }
  }

  public void setup() {
    ByteArrayOutputStream output = null;
    for (final Operation operation : getOperations()) {
      if ("Export".equals(operation.getType())) {
        System.out.println("Export / Import pair detected.");
        output = new ByteArrayOutputStream();
      } else if ("Import".equals(operation.getType()) && output == null) {
        System.out.println("Import / Export pair detected.");
        output = new ByteArrayOutputStream();
      }
      operation.setup(output);
    }
  }

  public void execute() {
    for (final Operation operation : operations) {
      final SummaryStatistics statistics = stats.get(operation.getName());
      long start = System.nanoTime();
      operation.execute();
      statistics.addValue((System.nanoTime() - start) / 1000000);
    }
  }

  public void tearDown() {
    for (final Operation operation : getOperations()) {
      operation.tearDown();
    }
  }

  public void printStats() {
    System.out.println(this.getName());
    for (final Operation operation : operations) {
      final SummaryStatistics statistics = stats.get(operation.getName());
      System.out.println(
          "\t"
              + operation.getName()
              + "(ms): "
              + statistics.getMean()
              + ", error="
              + calculateMeanCI(statistics, CI_LEVEL));
    }
  }

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
