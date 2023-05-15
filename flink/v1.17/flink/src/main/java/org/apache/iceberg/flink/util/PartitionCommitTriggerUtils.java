/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class PartitionCommitTriggerUtils {
  private PartitionCommitTriggerUtils() {}

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
          .optionalStart()
          .appendLiteral(" ")
          .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
          .optionalEnd()
          .optionalEnd()
          .toFormatter()
          .withResolverStyle(ResolverStyle.LENIENT);
  private static final DateTimeFormatter DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
          .toFormatter()
          .withResolverStyle(ResolverStyle.LENIENT);

  public static boolean isPartitionCommittable(
      long watermark, long partitionTime, String commitDelayString) {
    long commitDelay = toDuration(commitDelayString).toMillis();
    return watermark > partitionTime + commitDelay;
  }

  public static boolean isPartitionCommittable(
      long watermark,
      PartitionKey newPartitionKey,
      String commitDelayString,
      String watermarkZoneID,
      String extractorPattern,
      String formatterPattern) {
    ZoneId watermarkTimeZone = ZoneId.of(watermarkZoneID == null ? "UTC" : watermarkZoneID);
    LocalDateTime partitionTime =
        partitionTimeExtract(newPartitionKey, extractorPattern, formatterPattern);
    long epochPartTime = partitionTime.atZone(watermarkTimeZone).toInstant().toEpochMilli();
    return isPartitionCommittable(watermark, epochPartTime, commitDelayString);
  }

  public static LocalDateTime partitionTimeExtract(
      PartitionKey newPartitionKey, String extractorPattern, String formatterPattern) {
    String partitionPath;
    try {
      partitionPath =
          URLDecoder.decode(URLEncoder.encode(newPartitionKey.toPath(), "UTF-8"), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Invalid partition key: ", e);
    }
    List<String> partitionKeys = Lists.newArrayList();
    List<String> partitionValues = Lists.newArrayList();
    Iterable<String> partitionPathMap = Splitter.on('/').split(partitionPath);
    for (String s : partitionPathMap) {
      List<String> pair = Splitter.on('=').splitToList(s);
      partitionKeys.add(pair.get(0));
      try {
        partitionValues.add(URLDecoder.decode(pair.get(1), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Invalid partition key: ", e);
      }
    }

    String timestampString;
    if (extractorPattern == null) {
      timestampString = partitionValues.get(0);
    } else {
      timestampString = extractorPattern;
      for (int i = 0; i < partitionKeys.size(); i++) {
        timestampString =
            timestampString.replaceAll("\\$" + partitionKeys.get(i), partitionValues.get(i));
      }
    }
    return toLocalDateTime(timestampString, formatterPattern);
  }

  public static LocalDateTime toLocalDateTime(String timestampString, String formatterPattern) {
    if (formatterPattern == null) {
      return toLocalDateTimeDefault(timestampString);
    }

    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern(formatterPattern, Locale.ROOT);
    try {
      return LocalDateTime.parse(timestampString, dateTimeFormatter);
    } catch (DateTimeParseException e) {
      return LocalDateTime.of(
          LocalDate.parse(timestampString, dateTimeFormatter), LocalTime.MIDNIGHT);
    }
  }

  public static LocalDateTime toLocalDateTimeDefault(String timestampString) {
    try {
      return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
    } catch (DateTimeParseException e) {
      return LocalDateTime.of(LocalDate.parse(timestampString, DATE_FORMATTER), LocalTime.MIDNIGHT);
    }
  }

  public static Duration toDuration(String delayString) {
    if (delayString.endsWith("h") || delayString.endsWith("m") || delayString.endsWith("s")) {
      return Duration.parse("PT" + delayString.toUpperCase());
    } else if (delayString.endsWith("d")) {
      return Duration.parse("P" + delayString.toUpperCase());
    }

    throw new IllegalArgumentException("Invalid delay string: " + delayString);
  }
}
