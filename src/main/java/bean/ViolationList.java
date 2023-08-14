package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ViolationList {

     private Integer id;
     private String car;
     private String violation;
     private Long createTime;

}
