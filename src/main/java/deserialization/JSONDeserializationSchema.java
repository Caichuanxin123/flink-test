package deserialization;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONDeserializationSchema<T> implements DeserializationSchema<T> {
    private Class<T> clz;

    public JSONDeserializationSchema(Class<T> clz) {
        this.clz = clz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return JSON.parseObject(new String(message),clz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clz);
    }

}
