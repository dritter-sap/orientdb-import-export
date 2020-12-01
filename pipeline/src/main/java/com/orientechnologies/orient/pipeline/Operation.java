package com.orientechnologies.orient.pipeline;

import java.io.OutputStream;

public interface Operation {
  String getName();
  String getType();

  void setup(OutputStream output);
  void execute();
  void tearDown();
}
