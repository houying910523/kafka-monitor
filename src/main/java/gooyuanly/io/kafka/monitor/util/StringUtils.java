package gooyuanly.io.kafka.monitor.util;

import com.google.common.base.Joiner;

/**
 * @author hy
 * @date 2019/7/24
 * @desc
 */
public class StringUtils {

    private static final Joiner joiner = Joiner.on("\t");

    private StringUtils() {}

    public static String join(Object first, Object second, Object... rest) {
        return joiner.join(first, second, rest);
    }
}
