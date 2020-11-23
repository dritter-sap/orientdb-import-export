package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Importer {
  private static final int numberOfIterations = 25;
  private static final double CI_LEVEL = 0.95;
  private static final String databaseName = "testBench";

  private ODatabaseExport export;
  private ODatabaseImport importer;

  private OrientDB exportOrient;
  private OrientDB importOrient;

  private static ODatabaseSession exportDatabase;
  private static ODatabaseSession importDatabase;

  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  public static void main(String[] args) {
    final SummaryStatistics exportStatistics = new SummaryStatistics();
    final SummaryStatistics importStatistics = new SummaryStatistics();

    try (final ProgressBar pb = new ProgressBar("Iterations", numberOfIterations)) {
      for (int i = 0; i < numberOfIterations; i++) {
        // TODO: make configurable for atomic tasks
        emptyExportImportWorkload(exportStatistics, importStatistics);
        pb.step();
      }
      pb.stepTo(numberOfIterations);
    }
    System.out.println(
        "Export(ms): "
            + exportStatistics.getMean()
            + ", error="
            + calculateMeanCI(exportStatistics, CI_LEVEL));
    System.out.println(
        "Import(ms): "
            + importStatistics.getMean()
            + ", error="
            + calculateMeanCI(importStatistics, 0.95));
  }

  private static void emptyExportImportWorkload(SummaryStatistics exportStatistics, SummaryStatistics importStatistics) {
    final Importer importer = new Importer();
    importer.init();
    long start = System.currentTimeMillis();
    importer.exportDatabase();
    exportStatistics.addValue(System.currentTimeMillis() - start);
    start = System.currentTimeMillis();
    importer.importDatabase();
    importStatistics.addValue(System.currentTimeMillis() - start);
    importer.tearDown();
  }

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

  public void init() {
    final String exportDbUrl = "memory:target/export_" + Importer.class.getSimpleName();
    exportOrient = createDatabase(databaseName, exportDbUrl);

    exportDatabase = exportOrient.open(databaseName, "admin", "admin");
    try {
      exportDatabase.createClass("SimpleClass");
      export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) exportDatabase,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
    } catch (IOException e) {
      e.printStackTrace();
    }

    final String importDbUrl = "memory:target/import_" + Importer.class.getSimpleName();
    importOrient = createDatabase(databaseName, importDbUrl);

    importDatabase = importOrient.open(databaseName, "admin", "admin");
    try {
      ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
      importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) importDatabase,
              new ByteArrayInputStream(output.toByteArray()),
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void exportDatabase() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) exportDatabase);
    export.setOptions(" -excludeAll -includeSchema=true");
    export.exportDatabase();
  }

  public void importDatabase() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
    importer.importDatabase();
  }

  public void tearDown() {
    try {
      exportOrient.drop(databaseName);
      exportOrient.close();

      importOrient.drop(databaseName);
      importOrient.close();
    } catch (final Exception e) {
      System.out.println("Issues during teardown" + e.getMessage());
    }
  }

  private OrientDB createDatabase(String database, String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(database, ODatabaseType.PLOCAL);
    return orientDB;
  }
}
