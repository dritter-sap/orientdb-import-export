package com.orientechnologies.orient.pipeline;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class ExportOperation implements Operation {
  private ODatabaseExport export;
  private ODatabaseSession exportDatabase;
  private String name;
  private OrientDB orientDB;
  private String databaseName;

  public ExportOperation(final String name, final String databaseName) {
    this.name = name;
    this.databaseName = databaseName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return "Export";
  }

  @Override
  public void setup(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(final InputStream input) {
    final String exportDbUrl = "memory:target/export_" + Importer.class.getSimpleName();
    orientDB = createDatabase(databaseName, exportDbUrl);

    exportDatabase = orientDB.open(databaseName, "admin", "admin");
    try {
      exportDatabase.createClassIfNotExist("SimpleClass");
      export =
          new ODatabaseExport(
              (ODatabaseDocumentInternal) exportDatabase,
              System.out,
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
