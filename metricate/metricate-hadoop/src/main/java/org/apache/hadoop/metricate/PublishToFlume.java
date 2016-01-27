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

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PublishToFlume<RecordType extends SpecificRecord>
    extends Publisher<RecordType> {
  private static final Logger LOG = LoggerFactory.getLogger(PublishToFlume.class);

  private final Schema schema;
  private RpcClient client;
  private String host;
  private int port;
  private int batchSize;
  private BinaryEncoder encoder;

  public PublishToFlume(Schema schema) {
    super("PublishToFlume");
    this.schema = schema;
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration config = getConfig();
    host = config.getTrimmed(MetricateConstants.METRICATE_FLUME_HOST,
        "localhost");
    port = config.getInt(MetricateConstants.METRICATE_FLUME_PORT, 0);
    if (port == 0) {
      throw new IllegalArgumentException("No valid port set in " +
          MetricateConstants.METRICATE_FLUME_PORT);
    }
    batchSize = config.getInt(MetricateConstants.METRICATE_FLUME_BATCHSIZE,
        0);
    bind();
    super.serviceStart();
  }

  private void bind() {
    closeClient();
    client = RpcClientFactory.getDefaultInstance(host, port, batchSize);
  }

  @Override
  protected void serviceStop() throws Exception {
    closeClient();
    super.serviceStop();
  }

  /**
   * Close the client and set to null; can be used to re-open things.
   */
  private void closeClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  /**
   * Build a flume event from an avro record by serializing the avro
   * record inside it.
   * @param record record to serialize
   * @return an event to deliver
   * @throws IOException
   */
  private Event buildEvent(RecordType record) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      // create or re-use
      encoder = EncoderFactory.get().binaryEncoder(out, encoder);
      DatumWriter<RecordType> writer = new SpecificDatumWriter<>(
          schema);
      writer.write(record, encoder);
      encoder.flush();
      out.close();
      return EventBuilder.withBody(out.toByteArray());
    }
  }

  @Override
  protected void publish(List<RecordType> records) throws IOException {
    List<Event> events = new ArrayList<>(records.size());
    for (RecordType record : records) {
      events.add(buildEvent(record));
    }
    try {
      client.appendBatch(events);
    } catch (EventDeliveryException e) {
      LOG.warn("Failed to publish: {}", e, e);
      // clean up and recreate the client
      bind();
      throw new IOException(e);
    }
  }
}
