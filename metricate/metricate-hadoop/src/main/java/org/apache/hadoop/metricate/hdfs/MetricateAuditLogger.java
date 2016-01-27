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
import org.apache.hadoop.metricate.MetricateConstants;
import org.apache.hadoop.metricate.MetricateUtils;
import org.apache.hadoop.metricate.PublishToFlume;
import org.apache.hadoop.metricate.PublishToLog;
import org.apache.hadoop.metricate.Publisher;
import org.apache.hadoop.metricate.avro.FileStatusRecord;
import org.apache.hadoop.metricate.avro.NamenodeAuditEventRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;

/**
 * Log from HDFS to flume and/or local FS for testing/replay.
 * There's no stop() operation here, so it's not going to stop automatically
 * when used from HDFS. One is explicitly added for testing
 */
public class MetricateAuditLogger extends HdfsAuditLogger
    implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricateAuditLogger.class);
  public static final Schema SCHEMA = NamenodeAuditEventRecord.getClassSchema();

  List<Publisher<NamenodeAuditEventRecord>> publishers = new ArrayList<>(2);
  private PublishToFlume<NamenodeAuditEventRecord> flumePublisher;

  @Override
  public void initialize(Configuration conf) {
    publishers.add(new PublishToLog<>(SCHEMA));
    flumePublisher = new PublishToFlume<>(SCHEMA);
    publishers.add(flumePublisher);
    publishers.stream().forEach(p -> {
      p.init(conf);
      p.start();
    });
  }

  @Override
  public void close() throws Exception {
    publishers.stream().forEach(AbstractService::stop);
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

    LOG.debug("Audit event {}", cmd);
    NamenodeAuditEventRecord event = new NamenodeAuditEventRecord();
    event.setCommand(cmd);
    event.setUsername(userName);
//    event.setAddress(new StringBuffer(addr.getAddress()));

    if (stat != null) {
      FileStatusRecord fileStatus = MetricateUtils.createFileStatusRecord(stat);
      event.setSourceFileStatus(fileStatus);
    }
    long now = System.currentTimeMillis();
    event.setTimestamp(now);
    event.setDate(new Date(now).toString());
    event.setDest(nonNull(dst));
    event.setSucceeded(succeeded);
    publishers.stream().forEach(p -> p.put(event));
  }

  protected String nonNull(String dst) {
    return dst != null ? dst: "";
  }

  public long getEventsPublishedCount() {
    return flumePublisher.getEventsPublishedCount();
  }

  public long getPublishFailureCount() {
    return flumePublisher.getPublishFailureCount();
  }

  public static void registerAuditLogger(Configuration conf) {
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        MetricateConstants.METRICATE_AUDIT_LOGGER);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "MetricateAuditLogger{");
    sb.append("eventsPublishedCount=").append(getEventsPublishedCount());
    sb.append(", publishFailureCount=").append(getPublishFailureCount());
    sb.append('}');
    return sb.toString();
  }
}
