package com.orientechnologies.orient.pipeline;

public interface Operation {
  String getName();

  void setup();
  void execute();
  void tearDown();
}
