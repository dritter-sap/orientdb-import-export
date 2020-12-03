package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;

import java.io.*;

public class ImportOperation implements Operation {
  private ODatabaseImport importer;
  private ODatabaseSession importDatabase;
  private String databaseName;
  private byte[] input;
  private String operationName;

  private OrientDB orientDB;

  public ImportOperation(final String operationName, final String databaseName) {
    this.operationName = operationName;
    this.databaseName = databaseName;
  }

  public ImportOperation(final String name, final String databaseName, final byte[] input) {
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
  public void setup(final InputStream input) {
    final String importDbUrl = "memory:target/import_" + Importer.class.getSimpleName();
    orientDB = createDatabase(databaseName, importDbUrl);

    importDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
      System.out.println("Size=" + input.available());
      importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) importDatabase,
              input,
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
  public void execute() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
    importer.importDatabase();
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
