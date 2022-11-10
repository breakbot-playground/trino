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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryWithDifferentProjectIdConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final String alternateProjectCatalog = "bigquery";
    private static final String alternateProjectId = requireNonNull(System.getProperty("testing.alternate-bq-project-id"), "testing.alternate-bq-project-id system property not set");
    private static final String serviceAccountCatalog = "service_account_bigquery";

    protected BigQuerySqlExecutor bigQuerySqlExecutor;

    @BeforeClass(alwaysRun = true)
    public void initBigQueryExecutor()
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("bigquery.project-id", alternateProjectId),
                REQUIRED_TPCH_TABLES);
        queryRunner.createCatalog(serviceAccountCatalog, "bigquery", Map.of(
                "bigquery.views-enabled", "true",
                "bigquery.view-expire-duration", "30m"));
        return queryRunner;
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_RENAME_TABLE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testCreateSchemasInDifferentProjectIdCatalog()
    {
        // This test case would fail without the bug fix
        // It would create a schema in the wrong project, not the one defined in the catalog properties

        try {
            assertThat(getQueryRunner().execute("SHOW CATALOGS").getOnlyColumnAsSet())
                    .contains(alternateProjectCatalog, serviceAccountCatalog);

            getQueryRunner().execute("CREATE SCHEMA " + serviceAccountCatalog + ".service_account_schema");
            assertThat(getQueryRunner().execute("SHOW SCHEMAS FROM " + serviceAccountCatalog + " LIKE 'service_account_schema'").getOnlyValue())
                    .isEqualTo("service_account_schema");

            getQueryRunner().execute("CREATE SCHEMA " + alternateProjectCatalog + ".project_id_schema");
            assertThat(getQueryRunner().execute("SHOW SCHEMAS FROM " + alternateProjectCatalog + " LIKE 'project_id_schema'").getOnlyValue())
                    .isEqualTo("project_id_schema");
        }
        finally {
            getQueryRunner().execute("DROP SCHEMA IF EXISTS " + serviceAccountCatalog + ".service_account_schema");
            getQueryRunner().execute("DROP SCHEMA IF EXISTS " + alternateProjectCatalog + ".project_id_schema");
        }
    }

    @Test
    public void testNativeQuerySelectFromTestTable()
    {
        String suffix = randomNameSuffix();
        String tableName = alternateProjectCatalog + ".test.test_select" + suffix;
        String bigQueryTableName = "`" + alternateProjectId + "`.test.test_select" + suffix;
        try {
            getQueryRunner().execute("CREATE TABLE " + tableName + " (col BIGINT)");
            getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1), (2)");
            assertQuery(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + bigQueryTableName + "'))",
                    "VALUES 1, 2");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
