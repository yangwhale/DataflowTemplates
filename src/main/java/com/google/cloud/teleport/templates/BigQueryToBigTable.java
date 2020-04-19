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

package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.options.Description;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads BigQuery data and writes it to Datastore. The source data can be
 * either a BigQuery table or a SQL query.
 */
public class BigQueryToBigTable {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToBigTable.class);

  private static final byte[] FAMILY = Bytes.toBytes("cf_recommendations");

  /**
   * Custom PipelineOptions.
   */
  public interface BigQueryToBigTableOptions
      extends BigQueryReadOptions, ErrorWriteOptions {
        @Description("The Google Cloud project ID for the Cloud Bigtable instance.")
        String getBigtableProjectId();
      
        void setBigtableProjectId(String bigtableProjectId);
      
        @Description("The Google Cloud Bigtable instance ID .")
        String getBigtableInstanceId();
      
        void setBigtableInstanceId(String bigtableInstanceId);
      
        @Description("The Cloud Bigtable table ID in the instance." )
        String getBigtableTableId();
      
        void setBigtableTableId(String bigtableTableId);
  }

  static final DoFn<TableRow, Mutation> MUTATION_TRANSFORM = new DoFn<TableRow, Mutation>() {
        private static final long serialVersionUID = 1L;

        public byte[] getValueBytes(Map.Entry<String, Object> field) {
            switch(field.getKey()) {
                case "predicted_rating":
                    return ((Double) field.getValue()).toString().getBytes();
                case "user_id":
                    return ((String) field.getValue()).toString().getBytes();
                case "item_id":
                    return ((String) field.getValue()).toString().getBytes();
                case "rating_rank":
                    return ((String) field.getValue()).toString().getBytes();
                case "rating":
                    return ((Double) field.getValue()).toString().getBytes();
                default:
                    return ((String) field.getValue()).toString().getBytes();
            }
        }

        public byte[] getColumnBytes(TableRow row, Map.Entry<String, Object> field) {
            String rank = (String)row.get("rating_rank");
            if (field.getKey() == "item_id" || field.getKey() == "predicted_rating") {
                return (field.getKey()+rank).getBytes();
            } 

            return field.getKey().getBytes();
        }
    
        @ProcessElement
        public void processElement(DoFn<TableRow, Mutation>.ProcessContext c) throws Exception {
    
            TableRow row = c.element();
    
            //Use UUID for each HBase item's row key
            Put p = new Put(((String)row.get("user_id")).toString().getBytes());
    
            for (Map.Entry<String, Object> field : row.entrySet()) {
                // Skip unnecessary columns
                if (field.getKey() == "timestamp" || field.getKey() == "rating_rank" || field.getKey() == "user_id") {
                    continue;
                }

                LOG.info("[EYU] fields - [" + field.getKey() + "]: " + field.getValue());
                System.out.println("[EYU] fields - [" + field.getKey() + "]: " + field.getValue());
                p.addColumn(FAMILY, getColumnBytes(row, field), getValueBytes(field));
            }
            c.output(p);
    
        }
  };

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    

    BigQueryToBigTableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToBigTableOptions.class);

    // CloudBigtableTableConfiguration contains the project, instance and table to connect to.
    CloudBigtableTableConfiguration config =
        new CloudBigtableTableConfiguration.Builder()
        .withProjectId(options.getBigtableProjectId())
        .withInstanceId(options.getBigtableInstanceId())
        .withTableId(options.getBigtableTableId())
        .build();

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(BigQueryIO.readTableRows().from("youzhi-lab:movielens.recomm_ranked_filtered_short"))
        .apply(ParDo.of(MUTATION_TRANSFORM))
        .apply(CloudBigtableIO.writeToTable(config));

    pipeline.run();
  }
}
