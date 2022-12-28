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

package org.apache.flink.connector.jdbc.dialect.db2;

import org.apache.flink.test.util.AbstractTestBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Basic class for testing DB2 jdbc. */
@Testcontainers
public class Db2TestBaseITCase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Db2TestBaseITCase.class);

    @Container
    protected static final Db2Container DB2_CONTAINER =
            new Db2Container()
                    .withUsername("db2inst1")
                    .withPassword("flinkpw")
                    .withEnv("AUTOCONFIG", "false")
                    .withEnv("ARCHIVE_LOGS", "true")
                    .acceptLicense()
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                DB2_CONTAINER.getJdbcUrl(),
                DB2_CONTAINER.getUsername(),
                DB2_CONTAINER.getPassword());
    }
}
