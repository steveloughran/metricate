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

package org.apache.hadoop.metricate.testtools;

import org.apache.flume.node.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metricate.MetricateConstants;
import org.apache.hadoop.metricate.MetricateUtils;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Start a full flume instance in VM, including the RPC listener
 */
public class MiniFlumeService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(
      MiniFlumeService.class);

  public static final String FLUME_PORT = "flume.port";
  public static final String NAME = "a1";
  public static final String HOST = "localhost";

  private final Application agent = new Application();
  private final Map<String, String> props = new HashMap<>();
  private int port;

  public MiniFlumeService(String name) {
    super(name);
  }

  public void set(String key, String val) {
    props.put(getName() + "." + key, val);
  }

  public int getPort() {
    return port;
  }

  public Application getAgent() {
    return agent;
  }

  /*
  a1.channels = c1
a1.sources = r1
a1.sinks = k1

a1.channels.c1.type = memory

a1.sources.r1.channels = c1
a1.sources.r1.type = avro
# For using a thrift source set the following instead of the above line.
# a1.source.r1.type = thrift
a1.sources.r1.bind = 0.0.0.0
a1.sources.r1.port = 41414

a1.sinks.k1.channel = c1
a1.sinks.k1.type = logger
   */


  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String name = getName();
    port = conf.getInt(FLUME_PORT, 0);
    if (port == 0) {
      port = MetricateUtils.getPort(41414,5);
    }
    set("sources", "r1");
    set("channels", "c1");
    set("sinks", "k1");

    set("sources.r1.channels ", "c1");
    set("sources.r1.type", "avro");
    set("sources.r1.bind", HOST);
    set("sources.r1.port", Integer.toString(port));
    set("sinks.k1.channel", "c1");
    set("sinks.k1.type", "logger");
    set("channels.c1.type", "memory");

    conf.set(MetricateConstants.METRICATE_FLUME_HOST, HOST);
    conf.setInt(MetricateConstants.METRICATE_FLUME_PORT, port);
    conf.setInt(MetricateConstants.METRICATE_FLUME_BATCHSIZE, 1);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    props.entrySet().stream().forEach(e ->
        LOG.info("{} -> {}", e.getKey(), e.getValue()));
    ConfigurationProvider provider = new ConfigurationProvider(getName(), props);
    agent.handleConfigurationEvent(provider.getConfiguration());
    agent.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    agent.stop();
    super.serviceStop();
  }
}
