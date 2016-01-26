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

package org.apache.hadoop.metricate.hdfs;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.HdfsAuditLogger;
import org.apache.hadoop.metricate.PublishToFlume;
import org.apache.hadoop.metricate.PublishToLog;
import org.apache.hadoop.metricate.Publisher;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.metricate.avro.NamenodeAuditEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log from HDFS to flume and/or local FS for testing/replay.
 * There's no stop() operation here, so it's not going to stop automatically
 * when used from HDFS. One is explicitly added for testing
 */
public class MetricateAuditLogger extends HdfsAuditLogger
    implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricateAuditLogger.class);
  public static final Schema SCHEMA = NamenodeAuditEventRecord.getClassSchema();

  List<Publisher> publishers = new ArrayList<>(2);

  @Override
  public void initialize(Configuration conf) {
    publishers.add(new PublishToLog(SCHEMA));
    publishers.add(new PublishToFlume(SCHEMA));
    for (Publisher publisher : publishers) {
      publisher.init(conf);
      publisher.start();
    }
  }

  @Override
  public void close() throws Exception {
    for (Publisher publisher : publishers) {
      publisher.stop();
    }
  }

  @Override
  public void logAuditEvent(boolean succeeded,
      String userName,
      InetAddress addr,
      String cmd,
      String src,
      String dst,
      FileStatus stat,
      UserGroupInformation ugi,
      DelegationTokenSecretManager dtSecretManager) {
  }
}