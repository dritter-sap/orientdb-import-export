package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;

import java.io.*;

public class ImportExportOperation implements Operation {
  private ODatabaseImport importer;
  private ODatabaseSession importDatabase;

  private ODatabaseExport export;
  private ODatabaseSession exportDatabase;

  private String databaseName;
  private byte[] input;
  private String operationName;

  private OrientDB orientDB;

  public ImportExportOperation(final String operationName, final String databaseName) {
    this.operationName = operationName;
    this.databaseName = databaseName;
  }

  public ImportExportOperation(final String name, final String databaseName, final byte[] input) {
    this.operationName = name;
    this.databaseName = databaseName;
    this.input = input;
  }

  @Override
  public String getName() {
    return operationName;
  }

  @Override
  public String getType() {
    return "Import";
  }

  @Override
  public void setup(String path) {
    final String importDbUrl = "memory:target/import_" + Importer.class.getSimpleName();
    orientDB = createDatabase(databaseName, importDbUrl);

    importDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
      importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) importDatabase,
              path,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setup(final InputStream input, final OutputStream output) {
    final String importDbUrl = "memory:target/import_" + Importer.class.getSimpleName();
    orientDB = createDatabase(databaseName, importDbUrl);

    importDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
      System.out.println("Size(input)=" + input.available());
      importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) importDatabase,
              input,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });

      export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) importDatabase,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private OrientDB createDatabase(final String database, final String url) {
    final OrientDB orientDB = new OrientDB(url, OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(database, ODatabaseType.PLOCAL);
    return orientDB;
  }

  @Override
  public void executeImport() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
    importer.importDatabase();
  }

  @Override
  public void executeExport() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
    export.setOptions(" -excludeAll -includeSchema=true");
    export.exportDatabase();
  }

  @Override
  public void tearDown() {
    try {
      orientDB.drop(databaseName);
      orientDB.close();
    } catch (final Exception e) {
      System.out.println("Issues during teardown" + e.getMessage());
    }
  }

  public byte[] getInput() {
    return input;
  }
}
