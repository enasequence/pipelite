/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.cron;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Optional;
import lombok.extern.flogger.Flogger;

@Flogger
public abstract class CronUtils {

  private CronUtils() {}

  private static final CronDefinition unixDefinition =
      CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
  private static final CronDefinition quarzDefinition =
      CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
  private static final CronParser unixParser = new CronParser(unixDefinition);
  private static final CronParser quartzParser = new CronParser(quarzDefinition);

  /**
   * Parses the cron expression.
   *
   * @param cron the cron expression
   * @return the parsed cron expression
   * @throws IllegalArgumentException if the cron expression was not valid
   */
  private static Cron parse(String cron) {
    try {
      return unixParser.parse(cron);
    } catch (Exception ignored) {
    }
    try {
      return quartzParser.parse(cron);
    } catch (Exception ignored) {
    }
    throw new IllegalArgumentException("Invalid cron expression " + cron);
  }

  /**
   * Returns true if the cron expression is valid.
   *
   * @param cron the cron expression
   * @return true if the cron expression is valid
   */
  public static boolean validate(String cron) {
    try {
      parse(cron);
      return true;
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log(ex.getMessage());
      return false;
    }
  }

  /**
   * Returns a human readable description of the cron expression.
   *
   * @param cron the cron expression
   * @return a human readable description of the cron expression
   */
  public static String describe(String cron) {
    if (!validate(cron)) {
      return "invalid cron expression";
    }
    return CronDescriptor.instance(Locale.UK).describe(parse(cron));
  }

  /**
   * Returns the next launch time or null if the cron expression is not valid.
   *
   * @param cron the cron expression
   * @param startTime previous execution start time
   * @return the next launch time or null if the cron expression is not valid
   */
  public static ZonedDateTime launchTime(String cron, ZonedDateTime startTime) {
    if (!validate(cron)) {
      return null;
    }
    ExecutionTime executionTime = ExecutionTime.forCron(parse(cron));

    if (startTime != null) {
      ZonedDateTime now = ZonedDateTime.now();
      Optional<ZonedDateTime> next = executionTime.nextExecution(startTime);
      if (next.isPresent() && next.get().isAfter(now)) {
        return next.get();
      }
    }
    Optional<ZonedDateTime> next = executionTime.nextExecution(ZonedDateTime.now());
    if (next.isPresent()) {
      return next.get();
    }
    return null;
  }
}
