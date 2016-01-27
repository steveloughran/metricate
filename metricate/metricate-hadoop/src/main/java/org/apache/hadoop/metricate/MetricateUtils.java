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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.metricate.avro.FileStatusRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public final class MetricateUtils {
  private static final Logger LOG = LoggerFactory.getLogger(
      MetricateUtils.class);

  private static Random rand = new Random();

  private MetricateUtils() {
  }

  /**
   * Port scan & allocate is how most other apps find ports
   *
   * @param port given port
   * @param retries number of retries
   * @return
   * @throws IOException
   */
  public static int getPort(int port, int retries) throws IOException {
    int tryPort = port;
    int tries = 0;
    while (true) {
      if (tries > 0 || tryPort == 0) {
        tryPort = port + rand.nextInt(65535 - port);
      }
      if (tryPort == 0) {
        continue;
      }
      try (ServerSocket s = new ServerSocket(tryPort)) {
        LOG.info("Using port " + tryPort);
        return tryPort;
      } catch (IOException e) {
        tries++;
        if (tries >= retries) {
          LOG.info("Port is already in use; giving up");
          throw e;
        } else {
          LOG.info("Port is already in use; trying again");
        }
      }
    }
  }

  public static FileStatusRecord createFileStatusRecord(FileStatus stat) {
    FileStatusRecord fileStatus = new FileStatusRecord();
    fileStatus.setPath(stat.getPath().toString());
    fileStatus.setIsdir(stat.isDirectory());
    fileStatus.setLength(stat.getLen());
    fileStatus.setAccessed(stat.getAccessTime());
    fileStatus.setModified(stat.getModificationTime());
    fileStatus.setGroup(stat.getGroup());
    fileStatus.setOwner(stat.getOwner());
    fileStatus.setPermissions((int) stat.getPermission().toExtendedShort());
    return fileStatus;
  }
}
