package pipelite.configuration;

import com.beust.jcommander.Parameter;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;

@Data
@ConfigurationProperties(prefix = "pipelite.task")
public class TaskExecutorConfiguration {

    /**
     * Default amount of memory (GBytes) requested for task execution.
     */
    public int memory;

    /**
     * Default number of CPUs requested for task execution.
     */
    public int cores;
}
