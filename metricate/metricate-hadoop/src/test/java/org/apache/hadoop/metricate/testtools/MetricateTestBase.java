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

import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.StopWatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Base test case has a list of services which are stopped on teardown
 */
public abstract class MetricateTestBase extends Assert {
  private static final Logger LOG = LoggerFactory.getLogger(MetricateTestBase.class);

  @Rule
  public TestRule globalTimeout = new Timeout(30 * 1000);

  protected static List<Service> services = new LinkedList<>();

  protected static void addStartedService(Service s) {
    services.add(s);
  }

  @AfterClass
  public static void stopServices() {
    services.stream().forEach(Service::stop);
  }

  protected static void await(long time, BooleanSupplier probe, String text) {

    StopWatch watch = new StopWatch().start();
    while (watch.now(TimeUnit.MILLISECONDS) < time) {
      if (probe.getAsBoolean()) {
        return;
      }
    }
    // here? timeout
    fail(text);
  }


}
