import com.ke.bigdata.streaming.kafka.monitor.lag.LagService;

/**
 * @author hy
 * @date 2019/6/28
 * @desc
 */
public class LagServiceTest {

    public static void main(String[] args) throws InterruptedException {
        LagService service = new LagService("proxy-log-kafka01-matrix.zeus.lianjia.com:9092");
        service.registerGroupTopic("stream_sec-allproxy-www-log", "sec-allproxy-www-log");
        service.start();
        while(true) {
            service.snapshot().forEach(System.out::println);
            Thread.sleep(2000L);
        }
    }
}
