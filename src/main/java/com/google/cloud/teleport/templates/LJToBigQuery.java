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

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import java.util.ArrayList;
import java.util.List;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.regex.Pattern;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Templated pipeline to read text from TextIO, apply a javascript UDF to it, and write it to GCS.
 */
public class LJToBigQuery {

  /** Options supported by {@link TextIOToBigQuery}. */
  public interface Options extends DataflowPipelineOptions, JavascriptTextTransformerOptions {
    @Description("The GCS location of the text you'd like to process")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("JSON file with BigQuery Schema description")
    ValueProvider<String> getJSONPath();

    void setJSONPath(ValueProvider<String> value);

    @Description("Output topic to write to")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    //@Description("GCS path to javascript fn for transforming output")
    //ValueProvider<String> getJavascriptTextTransformGcsPath();

    //void setJavascriptTextTransformGcsPath(ValueProvider<String> jsTransformPath);

    //@Validation.Required
    //@Description("UDF Javascript Function Name")
    //ValueProvider<String> getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(
        ValueProvider<String> javascriptTextTransformFunctionName);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
  }

  private static final Logger LOG = LoggerFactory.getLogger(LJToBigQuery.class);

  private static final String BIGQUERY_SCHEMA = "BQ_Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
 //       .apply(
 //           TransformTextViaJavascript.newBuilder()
 //               .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
 //               .setFunctionName(options.getJavascriptTextTransformFunctionName())
 //               .build())
        
        .apply("Transform", ParDo.of(new DoFn<String, TableRow>(){

          private String[] columnNames = "craw_date,city,district,area,name,price,desc,pic,rid,hid".split(",");
          //private int ttlIvalidRecords = 0;

          public boolean isValidDate(String inDate) {

            // most likely invalid date string will be detected here and wouldn't go to date parsing below
            final Pattern pattern = Pattern.compile("^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$");
            if (!pattern.matcher(inDate).matches()) {
              return false;
            }

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setLenient(false);
            Date date = null;
            try {
              // parse would throw exception on "12-19" but not "9-12-19" so we still need to validate the year
              date = dateFormat.parse(inDate.trim());
            } catch (ParseException pe) {
                return false;
            }

            LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            int year  = localDate.getYear();
            if (year < 2000 || year > 3000) {
              return false;
            }

            return true;
          }

          @ProcessElement
          public void processElement(ProcessContext context) {

            TableRow row = new TableRow();
            String[] fields = context.element().split("\",\"");

            //LOG.info("[EYU] " + context.element() + ", " + columnNames.length + ", " + fields.length);

            if (fields.length != columnNames.length) {
              LOG.info("[EYU] CSV fields length invalid - (" + fields.length + ")," + context.element());
              //ttlIvalidRecords++;
              return;
            }

            // Check whether craw_date value is yyyy-MM-dd
            String craw_date = removeQuotationMarks(fields[0]);
            if (!isValidDate(craw_date)) {
              LOG.info("[EYU] craw_date format invalid - (" + craw_date + ")," + context.element());
              //ttlIvalidRecords++
              return;
            }
            
            for (int i = 0; i < fields.length; i++) {
              row.set(columnNames[i], removeQuotationMarks(fields[i]));
            }

            //LOG.info("[EYU] processed");

            context.output(row);
          }

          public String removeQuotationMarks(String value) {
            if (value.startsWith("\"")) {
              value = value.substring(1, value.length());
            }
            if (value.endsWith("\"")) {
              value = value.substring(0, value.length() - 1);
            }

            return value;
          }
        }))
//        .apply(BigQueryConverters.jsonToTableRow())
        .apply(
            "Insert into Bigquery",
            BigQueryIO.writeTableRows()
                .withSchema(
                    NestedValueProvider.of(
                        options.getJSONPath(),
                        new SerializableFunction<String, TableSchema>() {

                          @Override
                          public TableSchema apply(String jsonPath) {

                            TableSchema tableSchema = new TableSchema();
                            List<TableFieldSchema> fields = new ArrayList<>();
                            SchemaParser schemaParser = new SchemaParser();
                            JSONObject jsonSchema;

                            try {

                              jsonSchema = schemaParser.parseSchema(jsonPath);

                              JSONArray bqSchemaJsonArray =
                                  jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                              for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                TableFieldSchema field =
                                    new TableFieldSchema()
                                        .setName(inputField.getString(NAME))
                                        .setType(inputField.getString(TYPE));

                                if (inputField.has(MODE)) {
                                  field.setMode(inputField.getString(MODE));
                                }

                                fields.add(field);
                              }
                              tableSchema.setFields(fields);

                            } catch (Exception e) {
                              throw new RuntimeException(e);
                            }
                            return tableSchema;
                          }
                        }))
                .to(options.getOutputTable())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    pipeline.run();
  }
}
