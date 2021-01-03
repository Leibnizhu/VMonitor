package io.github.leibnizhu.vmonitor;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author Leibniz on 2020/12/23 5:14 PM
 */
public class JavaEntranceTest {

    @Test
    public void test() throws Exception {
        String ruleStr = Vertx.vertx().fileSystem().readFileBlocking("rule.json").toString();
        VMonitor endpoint1 = runOne(ruleStr);
        TimeUnit.SECONDS.sleep(10);
        VMonitor endpoint2 = runOne(ruleStr);
        TimeUnit.MINUTES.sleep(6);
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
                        endpoint.collect(new JsonObject().put(Constants$.MODULE$.EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME(), "serv-etl.SchedulerError"));
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
