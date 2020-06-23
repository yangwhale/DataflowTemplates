/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.cdc.merge;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for {@link MergeStatementBuilder}.
 */
@RunWith(JUnit4.class)
public class MergeStatementBuilderTest {
  static final String REPLICA_TABLE_ID = "myProject.myReplicaDataset.myTable";
  static final String STAGING_TABLE_ID = "myProject.myReplicaDataset.myTable_changelog";

  static final List<String> PRIMARY_KEY_COLUMNS = Arrays.asList("pk1", "pk2");
  static final MergeConfiguration MERGE_CONFIGURATION = MergeConfiguration.bigQueryConfiguration();

  static final List<String> FULL_COLUMN_LIST = Arrays.asList(
      "pk1", "pk2", "col1", "col2", "col3", MERGE_CONFIGURATION.timestampFieldName(),
      MERGE_CONFIGURATION.deletedFieldName());

  /**
   * Test case testFullMergeStatement.
   * The query for this test:
   *
   * MERGE `myProject.myReplicaDataset.myTable` AS replica
   * USING (SELECT pk1, pk2, col1, col2, col3, _metadata_timestamp, _metadata_deleted
   *     FROM (
   *         SELECT
   *             pk1, pk2, col1, col2, col3, _metadata_timestamp, _metadata_deleted,
   *             ROW_NUMBER() OVER
   *             (
   *                 PARTITION BY pk1, pk2 ORDER BY _metadata_timestamp DESC, _metadata_deleted ASC
   *             ) as row_num
   *             FROM `myProject.myReplicaDataset.myTable_changelog`
   *             WHERE _PARTITIONTIME >= TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -5 DAY))
   *                 OR _PARTITIONTIME IS NULL
   *         ) WHERE row_num=1
   *     ) AS staging
   * ON replica.pk1 = staging.pk1
   *    AND replica.pk2 = staging.pk2
   * WHEN MATCHED AND staging._metadata_deleted=True THEN DELETE
   * WHEN MATCHED AND replica._metadata_timestamp <= staging._metadata_timestamp
   *     THEN UPDATE SET pk1 = staging.pk1,
   *                     pk2 = staging.pk2,
   *                     col1 = staging.col1,
   *                     col2 = staging.col2,
   *                     col3 = staging.col3,
   *                     _metadata_timestamp = staging._metadata_timestamp,
   *                     _metadata_deleted = staging._metadata_deleted
   * WHEN NOT MATCHED BY TARGET AND staging._metadata_deleted!=True
   *     THEN INSERT(pk1, pk2, col1, col2, col3, _metadata_timestamp, _metadata_deleted)
   *         VALUES (staging.pk1, staging.pk2, staging.col1, staging.col2, staging.col3,
   *                 staging._metadata_timestamp, staging._metadata_deleted)
   */
  @Test
  public void testFullMergeStatement() {
    MergeStatementBuilder builder = new MergeStatementBuilder(MERGE_CONFIGURATION);
    String fullMergeStatement =
        builder.buildMergeStatement(
            REPLICA_TABLE_ID, STAGING_TABLE_ID,
            PRIMARY_KEY_COLUMNS, FULL_COLUMN_LIST);

    assertThat(fullMergeStatement,
        containsString(
            "SELECT "
                + "pk1, pk2, col1, col2, col3, _metadata_timestamp, _metadata_deleted, "
                + "ROW_NUMBER() OVER "
                + "(PARTITION BY pk1, pk2 ORDER BY _metadata_timestamp DESC, _metadata_deleted ASC)"
                + " as row_num FROM `myProject.myReplicaDataset.myTable_changelog` "
                + "WHERE _PARTITIONTIME >= TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)) "
                + "OR _PARTITIONTIME IS NULL"));

    assertThat(fullMergeStatement,
        containsString("ON "
            + "replica.pk1 = staging.pk1 "
            + "AND replica.pk2 = staging.pk2 "));

    assertThat(fullMergeStatement,
        containsString("MERGE `myProject.myReplicaDataset.myTable` AS replica"));

    assertThat(fullMergeStatement,
        containsString("THEN UPDATE SET "
            + "pk1 = staging.pk1, "
            + "pk2 = staging.pk2, "
            + "col1 = staging.col1, "
            + "col2 = staging.col2, "
            + "col3 = staging.col3, "
            + "_metadata_timestamp = staging._metadata_timestamp, "
            + "_metadata_deleted = staging._metadata_deleted "));

    assertThat(fullMergeStatement,
        containsString(
            "WHEN NOT MATCHED BY TARGET AND staging._metadata_deleted!=True THEN "
                + "INSERT(pk1, pk2, col1, col2, col3, _metadata_timestamp, _metadata_deleted) "
                + "VALUES ("
                + "staging.pk1, staging.pk2, staging.col1, staging.col2, staging.col3, "
                + "staging._metadata_timestamp, staging._metadata_deleted)"
        ));
  }

  /**
   * Test case testMergeStatementWithoutPartitionedTables.
   * The query for this test is similar to the previous one, except it does not have the
   * following section:
   *
   *             WHERE _PARTITIONTIME >= TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -5 DAY))
   *                 OR _PARTITIONTIME IS NULL
   */
  @Test
  public void testMergeStatementWithoutPartitionedTables() {
    MergeConfiguration config = MERGE_CONFIGURATION.toBuilder()
        .setSupportPartitionedTables(false)
        .build();
    MergeStatementBuilder builder = new MergeStatementBuilder(config);
    String fullMergeStatement =
        builder.buildMergeStatement(
            REPLICA_TABLE_ID, STAGING_TABLE_ID,
            PRIMARY_KEY_COLUMNS, FULL_COLUMN_LIST);

    assertThat(fullMergeStatement,
        containsString(
            "SELECT "
                + "pk1, pk2, col1, col2, col3, _metadata_timestamp, _metadata_deleted, "
                + "ROW_NUMBER() OVER "
                + "(PARTITION BY pk1, pk2 ORDER BY _metadata_timestamp DESC, _metadata_deleted ASC)"
                + " as row_num FROM `myProject.myReplicaDataset.myTable_changelog` )"));
  }
}
