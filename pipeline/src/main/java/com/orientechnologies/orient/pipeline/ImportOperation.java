package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ImportOperation implements Operation {
  private ODatabaseImport importer;
  private ODatabaseSession importDatabase;
  private String databaseName;
  private OrientDB orientDB;

  public ImportOperation(
      final OrientDB orientDB, final String databaseName) {
    this.orientDB = orientDB;
    this.databaseName = databaseName;
  }

  @Override
  public void setup() {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    importDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) importDatabase);
      importer =
          new ODatabaseImport(
              (ODatabaseDocumentInternal) importDatabase,
              new ByteArrayInputStream(((ByteArrayOutputStream) output).toByteArray()),
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {}
              });
    } catch (IOException e) {
      e.printStackTrace();
    }
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

  @Override
  public String getName() {
    return "Import";
  }
}
