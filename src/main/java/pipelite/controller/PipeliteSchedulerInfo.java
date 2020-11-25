package pipelite.controller;

import lombok.Builder;
import lombok.Value;
import pipelite.launcher.PipeliteSchedulerStats;

import java.util.List;

@Value
@Builder
public class PipeliteSchedulerInfo {
    private String schedulerName;
    private List<ScheduleInfo> schedules;

    @Value
    @Builder
    public static class ScheduleInfo {
        private String pipelineName;
        private String cron;
        private String description;
        private PipeliteSchedulerStats stats;
    }
}
