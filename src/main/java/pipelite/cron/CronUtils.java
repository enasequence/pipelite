/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Locale;
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

  private static Cron parse(String cronExpression) {
    try {
      return unixParser.parse(cronExpression);
    } catch (Exception ex) {
    }
    try {
      return quartzParser.parse(cronExpression);
    } catch (Exception ex) {
    }
    throw new RuntimeException("Invalid cron expression " + cronExpression);
  }

  public static boolean validate(String cronExpression) {
    try {
      parse(cronExpression);
      return true;
    } catch (Exception ex) {
      log.atSevere().log(ex.getMessage());
      return false;
    }
  }

  public static String describe(String cronExpression) {
    CronDescriptor descriptor = CronDescriptor.instance(Locale.UK);
    return descriptor.describe(parse(cronExpression));
  }

  public static LocalDateTime launchTime(String cronExpression) {
    ExecutionTime executionTime = ExecutionTime.forCron(parse(cronExpression));
    return executionTime.nextExecution(ZonedDateTime.now()).get().toLocalDateTime();
  }
}
