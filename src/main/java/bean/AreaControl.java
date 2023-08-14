package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AreaControl {

    private Integer id;
    private String areaId;
    private Integer carCount;
    private String windowStart;
    private String windowEnd;
}
