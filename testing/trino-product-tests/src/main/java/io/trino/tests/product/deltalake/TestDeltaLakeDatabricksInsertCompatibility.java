/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_104_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.Arrays.asList;

public class TestDeltaLakeDatabricksInsertCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private Optional<DatabricksVersion> databricksRuntimeVersion;

    @BeforeTestWithContext
    public void setup()
    {
        super.setUp();
        databricksRuntimeVersion = getDatabricksRuntimeVersion();
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testInsertCompatibility()
    {
        String tableName = "test_dl_insert_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, a_string STRING)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,'ala'), (2, 'kota')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'psa')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (4, 'lwa'), (5, 'jeza')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (6, 'wilka')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "psa"),
                    row(4, "lwa"),
                    row(5, "jeza"),
                    row(6, "wilka"));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPartitionedInsertCompatibility()
    {
        String tableName = "test_dl_partitioned_insert_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, a_string STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (a_number)" +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,'ala'), (2, 'kota')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'osla')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (4, 'lwa'), (5, 'jeza')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "osla"),
                    row(3, "psa"),
                    row(4, "bobra"),
                    row(4, "lwa"),
                    row(5, "jeza"));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoPartitionedDifferentOrderInsertCompatibility()
    {
        String tableName = "test_dl_trino_partitioned_different_order_insert_" + randomTableSuffix();

        onTrino().executeQuery("" +
                "CREATE TABLE delta.default." + tableName +
                "         (a_number INT, first VARCHAR, second VARCHAR)" +
                "         WITH (" +
                "         partitioned_by = ARRAY['second', 'first']," +
                "         location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "')");

        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'first', 'second')");

            List<Row> expectedRows = ImmutableList.of(row(1, "first", "second"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeltaPartitionedDifferentOrderInsertCompatibility()
    {
        String tableName = "test_dl_delta_partitioned_different_order_insert_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, first STRING, second STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (second, first)" +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'first', 'second')");

            List<Row> expectedRows = ImmutableList.of(row(1, "first", "second"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testInsertNonLowercaseColumnsCompatibility()
    {
        String tableName = "test_dl_insert_nonlowercase_columns_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (lower_case_string STRING, UPPER_CASE_STRING STRING, MiXeD_CaSe_StRiNg STRING)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES ('databricks', 'DATABRICKS', 'DaTaBrIcKs'), ('databricks', 'DATABRICKS', NULL)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (NULL, NULL, 'DaTaBrIcKs'), (NULL, NULL, NULL)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES ('trino', 'TRINO', 'TrInO'), ('trino', 'TRINO', NULL)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (NULL, NULL, 'TrInO'), (NULL, NULL, NULL)");

            List<Row> expectedRows = ImmutableList.of(
                    row("databricks", "DATABRICKS", "DaTaBrIcKs"),
                    row("databricks", "DATABRICKS", null),
                    row(null, null, "DaTaBrIcKs"),
                    row(null, null, null),
                    row("trino", "TRINO", "TrInO"),
                    row("trino", "TRINO", null),
                    row(null, null, "TrInO"),
                    row(null, null, null));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testInsertNestedNonLowercaseColumnsCompatibility()
    {
        String tableName = "test_dl_insert_nested_nonlowercase_columns_" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (an_int INT, nested STRUCT<lower_case_string: STRING, UPPER_CASE_STRING: STRING, MiXeD_CaSe_StRiNg: STRING>)" +
                "         USING delta " +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, struct('databricks', 'DATABRICKS', 'DaTaBrIcKs')), (2, struct('databricks', 'DATABRICKS', NULL))");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES " +
                    "(3, struct(NULL, NULL, 'DaTaBrIcKs'))," +
                    "(4, struct(NULL, NULL, NULL))");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (10, ROW('trino', 'TRINO', 'TrInO')), (20, ROW('trino', 'TRINO', NULL))");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (30, ROW(NULL, NULL, 'TrInO')), (40, ROW(NULL, NULL, NULL))");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "databricks", "DATABRICKS", "DaTaBrIcKs"),
                    row(2, "databricks", "DATABRICKS", null),
                    row(3, null, null, "DaTaBrIcKs"),
                    row(4, null, null, null),
                    row(10, "trino", "TRINO", "TrInO"),
                    row(20, "trino", "TRINO", null),
                    row(30, null, null, "TrInO"),
                    row(40, null, null, null));

            assertThat(onDelta().executeQuery("SELECT an_int, nested.lower_case_string, nested.UPPER_CASE_STRING, nested.MiXeD_CaSe_StRiNg FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT an_int, nested.lower_case_string, nested.UPPER_CASE_STRING, nested.MiXeD_CaSe_StRiNg FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testPartitionedInsertNonLowercaseColumnsCompatibility()
    {
        String tableName = "test_dl_partitioned_insert_nonlowercase_columns" + randomTableSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "         (lower_case_string STRING, UPPER_CASE_STRING STRING, MiXeD_CaSe_StRiNg STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (MiXeD_CaSe_StRiNg)" +
                "         LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES ('databricks', 'DATABRICKS', 'DaTaBrIcKs'), ('databricks', 'DATABRICKS', NULL)");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (NULL, NULL, 'DaTaBrIcKs'), (NULL, NULL, NULL)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES ('trino', 'TRINO', 'TrInO'), ('trino', 'TRINO', NULL)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (NULL, NULL, 'TrInO'), (NULL, NULL, NULL)");

            List<Row> expectedRows = ImmutableList.of(
                    row("databricks", "DATABRICKS", "DaTaBrIcKs"),
                    row("databricks", "DATABRICKS", null),
                    row(null, null, "DaTaBrIcKs"),
                    row(null, null, null),
                    row("trino", "TRINO", "TrInO"),
                    row("trino", "TRINO", null),
                    row(null, null, "TrInO"),
                    row(null, null, null));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteCompatibility()
    {
        String tableName = "test_delete_compatibility_" + randomTableSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName + " (a int, b int)" +
                " USING DELTA LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a % 2 = 0");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, 2),
                    row(3, 4),
                    row(5, 6));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCheckConstraintsCompatibility()
    {
        // CHECK constraint is not supported by Trino
        String tableName = "test_check_constraint_not_supported_" + randomTableSuffix();

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(id INT,  a_number INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
        onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT id_constraint CHECK (id < 100)");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " (id, a_number) VALUES (1, 1)");

            assertThat(onTrino().executeQuery("SELECT id, a_number FROM " + tableName))
                    .containsOnly(row(1, 1));

            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (2, 2)"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_number = 1"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a_number = 10 WHERE id = 1"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");

            assertThat(onTrino().executeQuery("SELECT id, a_number FROM " + tableName))
                    .containsOnly(row(1, 1));
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    /**
     * Smoke test compression when writing to a Delta table. It's verified that writer doesn't fail and reads succeed,
     * but it's not verified that compression actually takes place.
     */
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "compressionCodecs")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCompression(String compressionCodec)
    {
        testCompression(false, compressionCodec);
    }

    /**
     * Smoke test compression when writing to a Delta table using optimized writer. It's verified that writer doesn't fail
     * and reads succeed, but it's not verified that compression actually takes place.
     */
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "compressionCodecs")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCompressionWithOptimizedWriter(String compressionCodec)
    {
        testCompression(true, compressionCodec);
    }

    private void testCompression(boolean optimizedWriter, String compressionCodec)
    {
        String tableName = "test_compression" +
                (optimizedWriter ? "_optimized" : "") +
                "_" + compressionCodec +
                "_" + randomTableSuffix();
        String trinoTableName = "delta.default." + tableName;
        String location = "s3://" + bucketName + "/databricks-compatibility-test-" + tableName;

        onTrino().executeQuery("CREATE TABLE " + trinoTableName + " WITH (location = '" + location + "') " +
                "AS TABLE tpch.tiny.nation WITH NO DATA");

        try {
            onTrino().executeQuery("SET SESSION delta.parquet_optimized_writer_enabled = " + optimizedWriter);
            onTrino().executeQuery("SET SESSION delta.compression_codec = '" + compressionCodec + "'");

            if (optimizedWriter && "LZ4".equals(compressionCodec)) {
                // TODO (https://github.com/trinodb/trino/issues/9142) LZ4 is not supported with native Parquet writer
                assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO " + trinoTableName + " TABLE tpch.tiny.nation"))
                        .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Unsupported codec: LZ4");
            }
            else {
                onTrino().executeQuery("INSERT INTO " + trinoTableName + " TABLE tpch.tiny.nation");
                List<Row> expected = onTrino().executeQuery("TABLE tpch.tiny.nation").rows().stream()
                        .map(row -> row(row.toArray()))
                        .collect(toImmutableList());
                assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName))
                        .containsOnly(expected);

                if ("ZSTD".equals(compressionCodec) && databricksRuntimeVersion.orElseThrow().isOlderThan(DATABRICKS_104_RUNTIME_VERSION)) {
                    assertQueryFailure(() -> onDelta().executeQuery("SELECT * FROM default." + tableName))
                            .hasMessageContaining("java.lang.ClassNotFoundException: org.apache.hadoop.io.compress.ZStandardCodec");
                }
                else {
                    assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                            .containsOnly(expected);
                }
            }
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + trinoTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void verifyCompressionCodecsDataProvider()
    {
        assertThat(onTrino().executeQuery("SHOW SESSION LIKE 'delta.compression_codec'"))
                .containsOnly(row(
                        "delta.compression_codec",
                        "SNAPPY",
                        "SNAPPY",
                        "varchar",
                        "Compression codec to use when writing new data files. Possible values: " +
                                Stream.of(compressionCodecs())
                                        .map(arguments -> (String) getOnlyElement(asList(arguments)))
                                        .collect(toImmutableList())));
    }

    @DataProvider
    public Object[][] compressionCodecs()
    {
        return new Object[][] {
                {"NONE"},
                {"SNAPPY"},
                {"LZ4"},
                {"ZSTD"},
                {"GZIP"},
        };
    }

    @Test(groups = {DELTA_LAKE_OSS, DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testWritesToTableWithCheckConstraintFails()
    {
        String tableName = "test_writes_into_table_with_check_constraint_" + randomTableSuffix();
        try {
            onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT aIsPositive CHECK (a > 0)");

            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 3 WHERE b = 3"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 3"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = 42"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testMetadataOperationsRetainCheckConstraints()
    {
        String tableName = "test_metadata_operations_retain_check_constraints_" + randomTableSuffix();
        try {
            onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ADD CONSTRAINT aIsPositive CHECK (a > 0)");

            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " ADD COLUMN c INT");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".c IS 'example column comment'");
            onTrino().executeQuery("COMMENT ON TABLE delta.default." + tableName + " IS 'example table comment'");

            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2, 3)"))
                    .hasMessageContaining("Writing to tables with CHECK constraints is not supported");
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testWritesToTableWithGeneratedColumnFails()
    {
        String tableName = "test_writes_into_table_with_generated_column_" + randomTableSuffix();
        try {
            onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT, b BOOLEAN GENERATED ALWAYS AS (CAST(true AS BOOLEAN))) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

            onDelta().executeQuery("INSERT INTO default." + tableName + " (a) VALUES (1), (2), (3)");

            assertThat(onTrino().executeQuery("SELECT a, b FROM " + tableName))
                    .containsOnly(row(1, true), row(2, true), row(3, true));

            // Disallowing all statements just in case though some statements may not unrelated to generated columns
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, false)"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 3 WHERE b = true"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 3"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = false"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testWritesToTableWithCDFFails()
    {
        String tableName = "test_writes_into_table_with_CDF_" + randomTableSuffix();
        try {
            onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT, b INT) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'" +
                    "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            assertQueryFailure(() -> onTrino().executeQuery("UPDATE delta.default." + tableName + " SET a = 3 WHERE b = 3"))
                    .hasMessageContaining("Writing to tables with Change Data Feed enabled is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a = 3"))
                    .hasMessageContaining("Writing to tables with Change Data Feed enabled is not supported");
            assertQueryFailure(() -> onTrino().executeQuery("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = 42"))
                    .hasMessageContaining("Writing to tables with Change Data Feed enabled is not supported");
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2));
            assertThat(onDelta().executeQuery("SELECT a, b, _change_type, _commit_version FROM table_changes('default." + tableName + "', 0)"))
                    .containsOnly(row(1, 2, "insert", 1L));
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
