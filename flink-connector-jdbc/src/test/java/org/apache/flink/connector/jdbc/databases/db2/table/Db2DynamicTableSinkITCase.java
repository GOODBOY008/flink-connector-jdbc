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
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;

/** The Table Sink ITCase for {@link Db2Dialect}. */
public class Db2DynamicTableSinkITCase extends JdbcDynamicTableSinkITCase implements Db2TestBase {
    @Override
    protected TableRow createUpsertOutputTable() {
        return tableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", dbType("FLOAT"), DataTypes.FLOAT().notNull()),
                field("lencnt", dbType("FLOAT"), DataTypes.FLOAT().notNull()),
                pkField("cTag", dbType("INT"), DataTypes.INT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP(3)));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                field("id", dbType("INT"), DataTypes.INT().notNull()),
                field("num", dbType("INT"), DataTypes.INT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP(3)));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                field("NAME", dbType("VARCHAR(20)"), DataTypes.VARCHAR(20).notNull()),
                field("SCORE", dbType("INT"), DataTypes.INT().notNull()));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return tableRow(
                "checkpointTable", field("id", dbType("BIGINT"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected List<Row> testUserData() {
        return Arrays.asList(
                Row.of(
                        "user1",
                        "Tom",
                        "tom123@gmail.com",
                        new BigDecimal("8.1"),
                        new BigDecimal("16.2")),
                Row.of(
                        "user3",
                        "Bailey",
                        "bailey@qq.com",
                        new BigDecimal("9.99"),
                        new BigDecimal("19.98")),
                Row.of(
                        "user4",
                        "Tina",
                        "tina@gmail.com",
                        new BigDecimal("11.3"),
                        new BigDecimal("22.6")));
    }
}
