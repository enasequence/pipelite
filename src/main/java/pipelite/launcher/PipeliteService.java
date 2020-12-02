package pipelite.launcher;

import com.google.common.util.concurrent.AbstractScheduledService;

public abstract class PipeliteService extends AbstractScheduledService {
  @Override
  public abstract String serviceName();
}
