package deserialization;

import bean.MonitorInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class CsvDeserializationSchema implements DeserializationSchema<MonitorInfo> {

    @Override
    public MonitorInfo deserialize(byte[] message) throws IOException {
        String s1 = new String(message);
        String[] arr = s1.split(",");
        return new MonitorInfo(Long.parseLong(arr[0]),arr[1],arr[2],arr[3],Double.parseDouble(arr[4]),arr[5],arr[6]);
    }

    @Override
    public boolean isEndOfStream(MonitorInfo nextElement) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(MonitorInfo.class);
    }

}
