package io.github.leibnizhu.vmonitor;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author Leibniz on 2020/12/23 5:14 PM
 */
public class JavaEntranceTest {
    private Random rand = new Random();

    @Test
    public void clusterTest() throws Exception {
        String ruleStr = "[{\"name\":\"SchedulerError监控\",\"metric\":{\"name\":\"serv-etl.SchedulerError\",\"filter\":[],\"groupField\":[],\"groupAggFunc\":null,\"groupAggField\":\"key\",\"sampleInterval\":\"1m\",\"sampleAggFunc\":\"uniqueCount\",\"sampleAggField\":\"cost\"},\"period\":{\"every\":\"10s\",\"pend\":\"10s\"},\"condition\":{\"last\":\"30s\",\"method\":\"avg\",\"op\":\">=\",\"threshold\":3},\"alert\":{\"method\":\"WecomBot\",\"times\":3,\"interval\":\"90s\",\"config\":{\"token\":\"ab8bc9f4-573c-452d-9908-1db60ce326e7\",\"message\":\"SchedulerError最近90s出现3次以上\"}}}]";
        VMonitor endpoint1 = runOne(ruleStr);
        TimeUnit.SECONDS.sleep(10);
        VMonitor endpoint2 = runOne(ruleStr);
        TimeUnit.MINUTES.sleep(2);
        endpoint2.stop();
        for (int i = 0; i < 5; i++) {
            endpoint1.collect("serv-etl.SchedulerError", new JsonObject().put("cost", rand.nextInt(10)));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        VMonitor endpoint3 = runOne(ruleStr);
        TimeUnit.MINUTES.sleep(2);
        endpoint1.stop();
        endpoint3.stop();
    }

    private VMonitor runOne(String ruleStr) {
        VMonitor endpoint = VMonitor$.MODULE$.embedClusterVertx("test", "develop", ruleStr);
        Promise<Void> promise = Promise.promise();
        endpoint.startAsync(promise);
        promise.future()
                .onSuccess(v -> {
                    for (int i = 0; i < 10; i++) {
                        endpoint.collect("serv-etl.SchedulerError", new JsonObject().put("cost", rand.nextInt(10)));
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .onFailure(exp -> System.exit(1));
        return endpoint;
    }

}
