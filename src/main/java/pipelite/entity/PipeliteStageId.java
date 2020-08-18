package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteStageId implements Serializable {

    private String processId;
    private String processName;
    private String stageName;
}