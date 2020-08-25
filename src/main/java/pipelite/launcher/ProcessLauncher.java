package pipelite.launcher;

import com.google.common.util.concurrent.Service;

public interface ProcessLauncher extends Service {
  String serviceName();

  void init(String processId);

  int getTaskFailedCount();

  int getTaskSkippedCount();

  int getTaskCompletedCount();
}
