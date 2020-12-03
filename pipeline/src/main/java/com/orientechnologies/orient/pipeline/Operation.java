package com.orientechnologies.orient.pipeline;

import java.io.InputStream;

public interface Operation {
  String getName();
  String getType();

  void setup(String path);
  void setup(InputStream input);
  void execute();
  void tearDown();
}
