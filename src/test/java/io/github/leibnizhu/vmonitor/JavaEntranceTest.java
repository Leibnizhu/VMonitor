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
    public void test() throws Exception {
        String ruleStr = Vertx.vertx().fileSystem().readFileBlocking("rule.json").toString();
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
        endpoint2.start();
        TimeUnit.MINUTES.sleep(2);
        endpoint1.stop();
        endpoint2.stop();
    }

    private VMonitor runOne(String ruleStr) {
        VMonitor endpoint = new VMonitorEndpoint("test", "develop", ruleStr, null);
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
