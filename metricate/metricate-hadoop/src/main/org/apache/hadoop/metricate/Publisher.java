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

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Class to asynchronously publish an Avro record type to "something"; subclasses
 * get to choose what
 * @param <RecordType>
 */
public abstract class Publisher<RecordType extends SpecificRecord> extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);
  Executor publish = Executors.newSingleThreadExecutor();

  private final BlockingDeque<QueuedEvent> queue = new LinkedBlockingDeque<>();

  protected Publisher(String name) {
    super(name);
  }

  /**
   * Subclasses MUST call this after any of their own setup, as
   * it starts the publishing queue.
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    startPublishing();
  }

  @Override
  protected void serviceStop() throws Exception {
    // queue a stop event
    queue.add(new QueuedEvent());
    super.serviceStop();
  }

  protected void startPublishing() {
    LOG.info("Starting publisher");
    publish.execute(new Runnable() {
      @Override
      public void run() {
        while(!isInState(STATE.STOPPED)) {
          try {
            QueuedEvent event = queue.take();
            if (event.isStopEvent()) {
              LOG.info("stop event received");
              break;
            }
            try {
              publish(event.records);
            } catch (IOException e) {
              handlePublishFailure(event.records, e);
            }
          } catch (InterruptedException e) {
            // interruptions are ignored, but trigger a review of stopped state
            LOG.info("Interrupted", e);
          }
        }
      }
    });
  }

  public void put(List<RecordType> records) {
    queue.add(new QueuedEvent(records));
  }
  public void put(RecordType record) {
    ArrayList<RecordType> r = new ArrayList<>(1);
    r.add(record);
    put(r);
  }

  /**
   * Publish operation; this runs in the background thread.
   * @param records
   * @throws IOException
   */
  protected abstract void publish(List<RecordType> records) throws IOException;

  /**
   * Publish failed.
   * @param records
   */
  protected void handlePublishFailure(List<RecordType> records, Exception ex) {
    LOG.error("Failed to publish {} records", records.size());
    LOG.warn("{}", ex.toString(), ex);
  }

  private class QueuedEvent {
    public final String action;
    public final List<RecordType> records;

    /**
     * Queue a record for publishing
     * @param records
     */
    public QueuedEvent(List<RecordType> records) {
      this("publish", records);
    }

    /**
     * Empty constructor for stop actions
     */
    public QueuedEvent() {
      this("stop", null);
    }

    private QueuedEvent(String action, List<RecordType> records) {
      this.action = action;
      this.records = records;
    }

    public boolean isStopEvent() {
      return "stop".equals(action);
    }


  }

}
