package pipelite.schedule;

import org.junit.jupiter.api.Test;
import pipelite.time.Time;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduleTest {

  @Test
  public void lifecycle() {
    String pipelineName = "test";
    String cron = "0/1 * * * * ?"; // every second

    Schedule schedule = new Schedule(pipelineName);

    // Empty schedule.

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNull();
    assertThat(schedule.getCron()).isNull();

    // Set cron.

    schedule.setCron(cron);

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNull();
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Enable.
    ZonedDateTime first = ZonedDateTime.now();

    schedule.enable();

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfter(first);
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Wait until the schedule is executable.

    Time.wait(Duration.ofSeconds(1));

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isTrue();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfter(first);
    assertThat(schedule.getLaunchTime()).isBefore(ZonedDateTime.now());
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Disable the schedule.

    schedule.disable();

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNull();
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Enable.
    ZonedDateTime second = ZonedDateTime.now();

    schedule.enable();

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isFalse();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfter(second);
    assertThat(schedule.getCron()).isEqualTo(cron);

    // Wait until the schedule is executable.

    Time.wait(Duration.ofSeconds(1));

    assertThat(schedule.getPipelineName()).isEqualTo(pipelineName);
    assertThat(schedule.isExecutable()).isTrue();
    assertThat(schedule.getLaunchTime()).isNotNull();
    assertThat(schedule.getLaunchTime()).isAfter(second);
    assertThat(schedule.getLaunchTime()).isBefore(ZonedDateTime.now());
    assertThat(schedule.getCron()).isEqualTo(cron);
  }
}
