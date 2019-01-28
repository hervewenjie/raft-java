package com.github.wenweihu86.raft.example.server.service;

/**
 * Created by chengwenjie on 2017/5/9.
 */
public interface ExampleService {

    ExampleMessage.SetResponse set(ExampleMessage.SetRequest request);

    ExampleMessage.GetResponse get(ExampleMessage.GetRequest request);
}
