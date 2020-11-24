package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ExportOperation implements Operation {
  private ODatabaseExport export;
  private ODatabaseSession exportDatabase;
  private OrientDB orientDB;
  private String databaseName;

  public ExportOperation(
      final OrientDB orientDB, final String databaseName) {
    this.orientDB = orientDB;
    this.databaseName = databaseName;
  }

  @Override
  public String getName() {
    return "Export";
  }

  @Override
  public void setup() {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    exportDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      exportDatabase.createClassIfNotExist("SimpleClass");
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
  }

  @Override
  public void execute() {
    ODatabaseRecordThreadLocal.instance().set((ODatabaseDocumentInternal) exportDatabase);
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
}
