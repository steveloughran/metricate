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
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class PublishToLog<RecordType extends SpecificRecord>
    extends Publisher<RecordType> {
  private static final Logger LOG = LoggerFactory.getLogger(
      PublishToLog.class);

  private final Schema schema;

  public PublishToLog(Schema schema) {
    super("PublishToLog");
    this.schema = schema;
  }

  @Override
  protected void publish(List<RecordType> records) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      DatumWriter<RecordType> writer = new SpecificDatumWriter<>(schema);
      for (RecordType record : records) {
        writer.write(record, encoder);
      }
      encoder.flush();
      out.close();
      LOG.info(out.toString("UTF-8"));
    }
  }

}
