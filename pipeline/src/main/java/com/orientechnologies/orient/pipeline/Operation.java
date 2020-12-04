package com.orientechnologies.orient.pipeline;

import java.io.InputStream;
import java.io.OutputStream;

public interface Operation {
  String getName();
  String getType();

  void setup(String path);
  void setup(InputStream input, OutputStream output);
  void executeImport();
  void executeExport();
  void tearDown();
}
