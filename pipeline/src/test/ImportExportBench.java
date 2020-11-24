package com.orientechnologies.orient.pipeline;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(0)
public class ImportExportBench {
  private Importer importer;

  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("ImportExportBench.*")
            .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    new Runner(opt).run();
  }

  @Setup(Level.Iteration)
  public void setup() {
    importer = new Importer();
    importer.init();
  }

  @TearDown(Level.Iteration)
  public void tearDown() {
    importer.tearDown(workload);
  }

  @Benchmark
  public void exportEmpty() throws Exception {
    importer.exportDatabase();
  }

  // @Benchmark
  public void importEmpty() throws Exception {
    importer.importDatabase();
  }
}
