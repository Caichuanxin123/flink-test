package day2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data                 //get set
@NoArgsConstructor    //无参
@AllArgsConstructor   //有参
public class Order {

    /*随机生成订单ID(UUID)
    随机生成用户ID(0-2)
    随机生成订单金额(0-100)
    时间戳为当前系统时间*/

    private String orderId;
    private Integer userId;
    private Integer money;
    private Long ts;

}
