package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Importer {
  private final String databaseName = "testBench";

  private ODatabaseExport export;
  private ODatabaseImport importer;

  private OrientDB exportOrient;
  private OrientDB importOrient;

  private static ODatabaseSession exportDatabase;
  private static ODatabaseSession importDatabase;

  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  public static void main(String[] args) {
    final Importer importer = new Importer();
    importer.init();
    importer.exportDatabase();
    importer.importDatabase();
    importer.tearDown();
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
