package pipelite.cron;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import lombok.extern.flogger.Flogger;
import org.springframework.cglib.core.Local;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Locale;

@Flogger
public abstract class CronUtils {

  private CronUtils() {}

  private static final CronDefinition cronDefinition =
      CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);

  public static boolean validate(String expression) {
    CronParser parser = new CronParser(cronDefinition);
    try {
      parser.parse(expression);
      return true;
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Invalid cron expression %s", expression);
      return false;
    }
  }

  public static String describe(String expression) {
    CronParser parser = new CronParser(cronDefinition);
    CronDescriptor descriptor = CronDescriptor.instance(Locale.UK);
    return descriptor.describe(parser.parse(expression));
  }

  public static LocalDateTime launchTime(String expression) {
    CronParser parser = new CronParser(cronDefinition);
    ExecutionTime executionTime = ExecutionTime.forCron(parser.parse(expression));
    return executionTime.nextExecution(ZonedDateTime.now()).get().toLocalDateTime();
  }
}
