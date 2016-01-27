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

public class MetricateConstants {

  public static final String METRICATE_LOG_FILENAME = "metricate.log.filename";
  public static final String METRICATE_FLUME_HOST = "metricate.flume.hostname";
  public static final String METRICATE_FLUME_PORT = "metricate.flume.hostname";
  public static final String METRICATE_FLUME_BATCHSIZE
      = "metricate.flume.batchsize";

  public static final String METRICATE_AUDIT_LOGGER
      = "org.apache.hadoop.metricate.hdfs.MetricateAuditLogger";
}
