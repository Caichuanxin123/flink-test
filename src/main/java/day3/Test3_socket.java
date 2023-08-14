package day3;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Test3_socket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //我本地默认16
        env.setParallelism(2);

        env.addSource(new ParallelSourceFunction<String>() {

            private volatile boolean isRunning = true;
            private transient Socket currentSocket;
            private final String delimiter = "\n";
            private final long maxNumRetries = 0;
            private final long delayBetweenRetries = 500;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                final StringBuilder buffer = new StringBuilder();
                long attempt = 0;

                while (isRunning) {

                    try (Socket socket = new Socket()) {
                        currentSocket = socket;

                        socket.connect(new InetSocketAddress("hadoop10", 9999));
                        try (BufferedReader reader =
                                     new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                            char[] cbuf = new char[8192];
                            int bytesRead;
                            while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                                buffer.append(cbuf, 0, bytesRead);
                                int delimPos;
                                while (buffer.length() >= delimiter.length()
                                        && (delimPos = buffer.indexOf(delimiter)) != -1) {
                                    String record = buffer.substring(0, delimPos);
                                    // truncate trailing carriage return
                                    if (delimiter.equals("\n") && record.endsWith("\r")) {
                                        record = record.substring(0, record.length() - 1);
                                    }
                                    ctx.collect(record);
                                    buffer.delete(0, delimPos + delimiter.length());
                                }
                            }
                        }
                    }

                    // if we dropped out of this loop due to an EOF, sleep and retry
                    if (isRunning) {
                        attempt++;
                        if (maxNumRetries == -1 || attempt < maxNumRetries) {
                            Thread.sleep(delayBetweenRetries);
                        } else {
                            // this should probably be here, but some examples expect simple exists of the
                            // stream source
                            // throw new EOFException("Reached end of stream and reconnects are not
                            // enabled.");
                            break;
                        }
                    }
                }

                // collect trailing data
                if (buffer.length() > 0) {
                    ctx.collect(buffer.toString());
                }
            }

            @Override
            public void cancel() {

            }

        }).print();

        env.execute();
    }
}
