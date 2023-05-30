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

package org.apache.flink.connector.jdbc.databases.db2.table;

import org.apache.flink.connector.jdbc.databases.db2.Db2TestBase;
import org.apache.flink.connector.jdbc.databases.db2.dialect.Db2Dialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;

/** The Table Source ITCase for {@link Db2Dialect}. */
public class Db2DynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements Db2TestBase {

    @Override
    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("ID", dbType("INTEGER"), DataTypes.BIGINT().notNull()),
                field("BOOLEAN_C", dbType("BOOLEAN"), DataTypes.BOOLEAN()),
                field("SMALL_C", dbType("SMALLINT"), DataTypes.SMALLINT()),
                field("INT_C", dbType("INTEGER"), DataTypes.INT()),
                field("BIG_C", dbType("BIGINT"), DataTypes.BIGINT()),
                field("REAL_C", dbType("REAL"), DataTypes.FLOAT()),
                field("DOUBLE_C", dbType("DOUBLE"), DataTypes.DOUBLE()),
                field("NUMERIC_C", dbType("NUMERIC(10, 5)"), DataTypes.DECIMAL(10, 5)),
                field("DECIMAL_C", dbType("DECIMAL(10, 1)"), DataTypes.DECIMAL(10, 1)),
                field("VARCHAR_C", dbType("VARCHAR(200)"), DataTypes.STRING()),
                field("CHAR_C", dbType("CHAR"), DataTypes.CHAR(1)),
                field("CHARACTER_C", dbType("CHAR(3)"), DataTypes.CHAR(3)),
                field("TIMESTAMP_C", dbType("TIMESTAMP"), DataTypes.TIMESTAMP(3)),
                field("DATE_C", dbType("DATE"), DataTypes.DATE()),
                field("TIME_C", dbType("TIME"), DataTypes.TIME(0)),
                field("DEFAULT_NUMERIC_C", dbType("NUMERIC"), DataTypes.DECIMAL(10, 0)),
                field("TIMESTAMP_PRECISION_C", dbType("TIMESTAMP(9)"), DataTypes.TIMESTAMP(9)));
    }

    @Override
    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        true,
                        (short) 32767,
                        65535,
                        2147483647,
                        5.5f,
                        6.6d,
                        123.12345,
                        404.4,
                        "Hello World",
                        "a",
                        "abc",
                        LocalDateTime.parse("2020-07-17T18:00:22.123"),
                        LocalDate.parse("2020-07-17"),
                        LocalTime.parse("18:00:22"),
                        500,
                        LocalDateTime.parse("2020-07-17T18:00:22.123456789")),
                Row.of(
                        2L,
                        false,
                        (short) 32767,
                        65535,
                        2147483647,
                        5.5f,
                        6.6d,
                        123.12345,
                        404.4,
                        "Hello World",
                        "a",
                        "abc",
                        LocalDateTime.parse("2020-07-17T18:00:22.123"),
                        LocalDate.parse("2020-07-17"),
                        LocalTime.parse("18:00:22"),
                        500,
                        LocalDateTime.parse("2020-07-17T18:00:22.123456789")));
    }
}
