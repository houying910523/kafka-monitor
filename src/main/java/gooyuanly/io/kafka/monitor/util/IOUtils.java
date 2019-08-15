package gooyuanly.io.kafka.monitor.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class IOUtils {

    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            //do nothing
        }
    }
}
