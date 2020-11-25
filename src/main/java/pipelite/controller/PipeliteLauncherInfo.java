package pipelite.controller;

import lombok.Builder;
import lombok.Value;
import pipelite.launcher.PipeliteLauncherStats;

@Value
@Builder
public class PipeliteLauncherInfo {
    private String launcherName;
    private String pipelineName;
    private PipeliteLauncherStats stats;
}
