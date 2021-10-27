package cn._51doit.day10;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * 在pom文件中添加异步httpClient的依赖
 *
 *       <!-- 高效的异步HttpClient -->
 * 		<dependency>
 * 			<groupId>org.apache.httpcomponents</groupId>
 * 			<artifactId>httpasyncclient</artifactId>
 * 			<version>4.1.4</version>
 * 		</dependency>
 *
 */
public class AsyncHttpQueryFunction extends RichAsyncFunction<String, OrderBean> {

    private String key = "00a418d5be2b6dcc72d2a145c2e92213";

    private transient CloseableHttpAsyncClient httpclient;

    //使用HttpClient进行异步查询
    @Override
    public void open(Configuration parameters) throws Exception {
        //创建异步查询的HTTPClient
        //创建一个异步的HttpClient连接池
        //初始化异步的HttpClient
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpclient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig)
                .build();

        httpclient.start();

    }

    @Override
    public void asyncInvoke(String line, ResultFuture<OrderBean> resultFuture) throws Exception {

        try {
            OrderBean orderBean = JSON.parseObject(line, OrderBean.class);

            //异步查询
            double longitude = orderBean.longitude;
            double latitude = orderBean.latitude;
            HttpGet httpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?&location="+ longitude+"," + latitude +"&key=" + key);
            //查询返回Future
            Future<HttpResponse> future = httpclient.execute(httpGet, null);

            //从Future中取数据
            CompletableFuture.supplyAsync(new Supplier<OrderBean>() {

                @Override
                public OrderBean get() {
                    try {
                        HttpResponse response = future.get();
                        String province = null;
                        String city = null;
                        if (response.getStatusLine().getStatusCode() == 200) {
                            //获取请求的json字符串
                            String result = EntityUtils.toString(response.getEntity());
                            //System.out.println(result);
                            //转成json对象
                            JSONObject jsonObj = JSON.parseObject(result);
                            //获取位置信息
                            JSONObject regeocode = jsonObj.getJSONObject("regeocode");
                            if (regeocode != null && !regeocode.isEmpty()) {
                                JSONObject address = regeocode.getJSONObject("addressComponent");
                                //获取省市区
                                province = address.getString("province");
                                city = address.getString("city");
                                //String businessAreas = address.getString("businessAreas");
                            }
                        }
                        orderBean.province = province;
                        orderBean.city = city;
                        return orderBean;
                    } catch (Exception e) {
                        // Normally handled explicitly.
                        return null;
                    }
                }
            }).thenAccept((OrderBean result) -> {
                resultFuture.complete(Collections.singleton(result));
            });

        } catch (Exception e) {
            resultFuture.complete(Collections.singleton(null));
        }
    }

    @Override
    public void close() throws Exception {
        httpclient.close();
    }
}
