/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metricate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metricate.avro.FileStatusRecord;
import org.apache.hadoop.metricate.testtools.MetricateTestBase;
import org.apache.hadoop.metricate.testtools.MiniFlumeService;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.*;

public class TestMiniFlumeService extends MetricateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestMiniFlumeService.class);

  private static MiniFlumeService flume;

  @BeforeClass
  public static void createFlumeService() {
    flume = new MiniFlumeService("f1");
    flume.init(new Configuration());
    flume.start();
    addStartedService(flume);
  }

  @Test
  public void testFlumeHasPort() throws Throwable {
    assertTrue(flume.getPort() != 0);
  }

  @Test
  public void testFlumeCreated() throws Throwable {
    assertNotNull(flume.getAgent());
  }

  @Test
  public void testPublish() throws Throwable {
    PublishToFlume<FileStatusRecord> publish = new PublishToFlume<>(
        FileStatusRecord.getClassSchema());
    publish.init(flume.getConfig());
    publish.start();
    FileStatus status = new FileStatus(
        0, true, 1, 1000, System.currentTimeMillis(),
        new Path("hdfs://localhost:8080/"));
    publish.put(MetricateUtils.createFileStatusRecord(status));
    LOG.info("Awaiting published events");
    await(30000, () -> publish.getEventsPublishedCount() > 0, "not published");
    publish.stop();
    assertThat("publishedCount", publish.getEventsPublishedCount(), equalTo(1L));
    assertThat("failedPublishedCount", publish.getPublishFailureCount(), equalTo(0L));

  }
}
