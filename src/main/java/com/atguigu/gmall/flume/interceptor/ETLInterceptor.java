package com.atguigu.gmall.flume.interceptor;

import com.atguigu.gmall.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author WUBIN
 * @create 2022-10-17 23:51
 */
public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }
/*
*校验json格式是否完整
 */

    @Override
    public Event intercept(Event event) {
//        1.获取body当中的数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
//        2.校验json格式是否完整
        if(JSONUtil.isJSONValidate(log)){
            return event;
        }else{
            return null;
        }

    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()){
            Event event= iterator.next();
            if(intercept(event)==null){
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
