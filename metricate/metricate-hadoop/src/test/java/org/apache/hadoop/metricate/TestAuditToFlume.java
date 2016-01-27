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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.apache.hadoop.metricate.hdfs.MetricateAuditLogger;
import org.apache.hadoop.metricate.testtools.MetricateTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;

public class TestAuditToFlume extends MetricateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestAuditToFlume.class);

  protected static MiniDFSCluster hdfs;
  private static URI hdfsURI;
  private static FileSystem dfs;

  @BeforeClass
  public static void startServices() throws IOException {
    startFlumeService();
    Configuration config = flume.getConfig();
    MetricateAuditLogger.registerAuditLogger(config);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(config);
    hdfs = builder.build();
    hdfs.waitClusterUp();
    hdfsURI = hdfs.getURI();
    dfs = hdfs.getFileSystem();
  }

  @AfterClass
  public static void stopMinicluster() {
    if (hdfs != null) {
      LOG.info("Shutting down HDFS cluster");
      hdfs.shutdown();
    }
  }

  private MetricateAuditLogger getAuditLogger() {
    List<AuditLogger> auditLoggers = hdfs.getNameNode()
        .getNamesystem()
        .getAuditLoggers();
    auditLoggers.stream().forEach(l -> LOG.info(l.toString()));
    List<MetricateAuditLogger> mal = auditLoggers.stream()
        .filter(l -> l instanceof MetricateAuditLogger)
        .map(l -> (MetricateAuditLogger) l).collect(Collectors.toList());
    assertThat("logger", mal, hasSize(1));
    return mal.get(0);

  }

  @Test
  public void testHDFSOperations() throws Throwable {
    int ops = 0;
    dfs.mkdirs(new Path("/"));
    ops++;
    Path p1 = new Path("/path1");
    Path p2 = new Path("/path2");
    Path p3 = new Path("/path3");
    ContractTestUtils.touch(dfs, p1);
    try {
      ops++;
      dfs.create(p1, false);
    } catch (IOException expected) {

    }
    ops++;
    dfs.rename(p1, p2);
    try {
      ops++;
      dfs.rename(p1, p2);
    } catch (FileNotFoundException e) {

    }
    ops++;
    dfs.mkdirs(p3);
    ops++;
    dfs.rename(p2, p3);
    MetricateAuditLogger auditLogger = getAuditLogger();
    final int l = ops;
    await(10000,
        () -> auditLogger.getEventsPublishedCount() == l,
        () -> "Wrong #of published events" + auditLogger + " expected " + l);

  }

}
