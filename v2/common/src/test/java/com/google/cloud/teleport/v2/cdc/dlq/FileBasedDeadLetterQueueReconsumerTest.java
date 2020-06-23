/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.cdc.dlq;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the FileBasedDeadLetterQueueReconsumer transform and components. */
@RunWith(JUnit4.class)
public class FileBasedDeadLetterQueueReconsumerTest {

  private static final String[] JSON_FILE_CONTENTS_1 =
      {"{\"data\":\"datasample1\"}",
      "{\"data\":\"datasample2\"}",
      "{\"data\":\"datasample3\"}"};

  static final Logger LOG = LoggerFactory.getLogger(FileBasedDeadLetterQueueReconsumerTest.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public TestPipeline p = TestPipeline.create();

  private String createJsonFile(String filename, String[] fileLines) throws IOException {
    File f = folder.newFile(filename);
    FileWriter w = new FileWriter(f);
    for (String line: fileLines) {
      w.write(line);
      w.write('\n');
    }
    w.close();
    return f.getAbsolutePath();
  }

  @Test
  public void testFilesAreConsumed() throws IOException {
    String fileName = createJsonFile("dlqFile1.json", JSON_FILE_CONTENTS_1);
    folder.newFolder("tmp");

    String folderPath = Paths.get(folder.getRoot().getAbsolutePath()).resolve("*").toString();
    PCollection<String> jsonData = p
        .apply(FileIO.match()
            .filepattern(folderPath))
        .apply(FileBasedDeadLetterQueueReconsumer.moveAndConsumeMatches());
    PAssert.that(jsonData).containsInAnyOrder(JSON_FILE_CONTENTS_1);
    p.run().waitUntilFinish();

    assertFalse(new File(fileName).exists());
  }

  @Test
  public void testAllFilesAreConsumed() throws IOException {
    TestStream<String> inputFiles = TestStream.create(StringUtf8Coder.of())
        .addElements(
            createJsonFile("dlqFile1.json", JSON_FILE_CONTENTS_1),
            createJsonFile("dlqFile2.json", JSON_FILE_CONTENTS_1))
        .addElements(createJsonFile("dlqFile3.json", JSON_FILE_CONTENTS_1))
        .advanceWatermarkToInfinity();

    PCollection<String> jsonData = p.apply(inputFiles)
        .apply(FileIO.matchAll())
        .apply(FileBasedDeadLetterQueueReconsumer.moveAndConsumeMatches());

    PAssert.that(jsonData)
        .containsInAnyOrder(
            Stream.of(JSON_FILE_CONTENTS_1)
                .flatMap(line -> Stream.of(line, line, line))
                .collect(Collectors.toList()));

    p.run().waitUntilFinish();
  }

}
