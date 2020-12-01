package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.db.*;
import me.tongfei.progressbar.ProgressBar;

public class Importer {
  private static final int numberOfIterations = 25;
  private static final String databaseName = "testBench";

  public static void main(String[] args) {
    final String exportDbUrl = "memory:target/export_" + Importer.class.getSimpleName();
    final OrientDB exportOrient = createDatabase(databaseName, exportDbUrl);
    final Operation exportOperation =
        new ExportOperation("EmptyExport", exportOrient, databaseName);

    final String importDbUrl = "memory:target/import_" + Importer.class.getSimpleName();
    final OrientDB importOrient = createDatabase(databaseName, importDbUrl);
    final Operation importOperation =
        new ImportOperation("EmptyImport", importOrient, databaseName);

    final Workload workload = new Workload("Empty Import/Export", exportOperation, importOperation);
    final Importer importer = new Importer();
    importer.execute(workload, numberOfIterations);
  }

  private void execute(final Workload workload, final int numberOfIterations) {
    try (final ProgressBar pb = new ProgressBar("Iterations", numberOfIterations)) {
      workload.setup();
      for (int i = 0; i < numberOfIterations; i++) {
        executeWorkload(workload);
        pb.step();
      }
      pb.stepTo(numberOfIterations);
    }
    workload.printStats();
    workload.tearDown();
  }

  private void executeWorkload(final Workload workload) {
    workload.setup();
    workload.execute();
  }

  private static OrientDB createDatabase(final String database, final String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(database, ODatabaseType.PLOCAL);
    return orientDB;
  }
}
