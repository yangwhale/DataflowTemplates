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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class BigQueryMerger.
 */
public class BigQueryMerger extends PTransform<PCollection<MergeInfo>, PCollection<Void>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStatementIssuingFn.class);

  private Duration windowDuration;
  private BigQuery testBigQueryClient;
  private MergeConfiguration mergeConfiguration;

  public BigQueryMerger(
      Duration windowDuration, BigQuery testBigQueryClient, MergeConfiguration mergeConfiguration) {
    this.windowDuration = windowDuration;
    this.testBigQueryClient = testBigQueryClient;
    this.mergeConfiguration = mergeConfiguration;
  }

  @Override
  public PCollection<Void> expand(PCollection<MergeInfo> input) {
    final MergeStatementBuilder mergeBuilder = new MergeStatementBuilder(mergeConfiguration);
    return input
        .apply(
            MapElements.into(
                TypeDescriptors.kvs(
                    TypeDescriptors.strings(), TypeDescriptor.of(MergeInfo.class)))
                .via(mergeInfo -> KV.of(mergeInfo.getReplicaTable(), mergeInfo)))
        .apply(new TriggerPerKeyOnFixedIntervals<String, MergeInfo>(windowDuration))
        .apply(Values.create())
        .apply(MapElements.into(TypeDescriptors.strings()).via(mergeInfo -> {
          return mergeBuilder.buildMergeStatement(
              mergeInfo.getReplicaTable(),
              mergeInfo.getStagingTable(),
              mergeInfo.getAllPkFields(),
              mergeInfo.getAllFields());
        }))
        .apply(ParDo.of(new BigQueryStatementIssuingFn(this.testBigQueryClient)))
        .apply(
            MapElements.into(TypeDescriptors.voids())
                .via(
                    whatever ->
                        (Void) null)); // TODO(pabloem) Remove this line and find a return type
  }

  /**
   * Class {@link TriggerPerKeyOnFixedIntervals}.
   * @param <K> key.
   * @param <V> value.
   */
  public static class TriggerPerKeyOnFixedIntervals<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

    private Duration intervalDuration;

    TriggerPerKeyOnFixedIntervals(Duration intervalDuration) {
      this.intervalDuration = intervalDuration;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      return input
          .apply(
              Window.<KV<K, V>>into(new GlobalWindows())
                  .discardingFiredPanes()
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(Duration.ZERO)
                              .alignedTo(intervalDuration, org.joda.time.Instant.now()))))
          .apply(GroupByKey.create())
          .apply(
              ParDo.of(
                  new DoFn<KV<K, Iterable<V>>, KV<K, V>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                      LOG.debug(
                          "TS: {} | Element: {} | Pane: {}", c.timestamp(), c.element(), c.pane());
                      Iterator<V> it = c.element().getValue().iterator();
                      if (it.hasNext()) {
                        c.output(KV.of(c.element().getKey(), it.next()));
                      }
                    }
                  }));
    }
  }

  /**
   * Class {@link BigQueryStatementIssuingFn}.
   */
  public class BigQueryStatementIssuingFn extends DoFn<String, Void> {

    public static final String JOB_ID_PREFIX = "bigstream_to_bq";

    private BigQuery bigQueryClient;

    BigQueryStatementIssuingFn(BigQuery bigQueryClient) {
      this.bigQueryClient = bigQueryClient;
    }

    @Setup
    public void setUp() {
      if (bigQueryClient == null) {
        bigQueryClient = BigQueryOptions.getDefaultInstance().getService();
      }
    }

    @Override
    public TypeDescriptor getInputTypeDescriptor() {
      return TypeDescriptor.of(MergeInfo.class);
    }

    @ProcessElement
    public void process(ProcessContext c) throws InterruptedException {
      String statement = c.element();
      Job jobInfo = issueQueryToBQ(statement);
      LOG.info("Job Info for triggered job: {}", jobInfo);
      jobInfo = jobInfo.waitFor();
      LOG.info("Job Info for finalized job: {}", jobInfo);
    }

    private Job issueQueryToBQ(String statement) throws InterruptedException {
      QueryJobConfiguration jobConfiguration = QueryJobConfiguration.newBuilder(statement).build();

      String jobId = makeJobId(JOB_ID_PREFIX, statement);

      LOG.info("Triggering job {} for statement |{}|", jobId, statement);

      TableResult result = bigQueryClient.query(jobConfiguration, JobId.of(jobId));
      return bigQueryClient.getJob(JobId.of(jobId));
    }

    String makeJobId(String jobIdPrefix, String statement) {
      DateTimeFormatter formatter =
          DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ssz").withZone(ZoneId.of("UTC"));
      String randomId = UUID.randomUUID().toString();
      return String.format(
          "%s_%d_%s_%s",
          jobIdPrefix, Math.abs(statement.hashCode()), formatter.format(Instant.now()), randomId);
    }
  }
}
