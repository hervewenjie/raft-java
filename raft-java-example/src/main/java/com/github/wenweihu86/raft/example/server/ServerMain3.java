package com.github.wenweihu86.raft.example.server;

import com.github.wenweihu86.raft.RaftNode;
import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.example.server.service.ExampleService;
import com.github.wenweihu86.raft.example.server.service.impl.ExampleServiceImpl;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.service.RaftClientService;
import com.github.wenweihu86.raft.service.RaftConsensusService;
import com.github.wenweihu86.raft.service.impl.RaftClientServiceImpl;
import com.github.wenweihu86.raft.service.impl.RaftConsensusServiceImpl;
import com.github.wenweihu86.rpc.server.RPCServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chengwenjie on 2017/5/9.
 */
public class ServerMain3 {

    public static void main(String[] args) {

        args = new String[3];
        args[0] = "./data";
        args[1] = "127.0.0.1:8051:1,127.0.0.1:8052:2,127.0.0.1:8053:3";
        args[2] = "127.0.0.1:8053:3";

        // parse args
        if (args.length != 3) {
            System.out.printf("Usage: ./run_server.sh DATA_PATH CLUSTER CURRENT_NODE\n");
            System.exit(-1);
        }
        // arg[0]: raft data dir
        String dataPath = args[0];
        // arg[1]: peers, format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[1];
        String[] splitArray = servers.split(",");
        List<RaftMessage.Server> serverList = new ArrayList<>();
        for (String serverString : splitArray) {
            RaftMessage.Server server = parseServer(serverString);
            serverList.add(server);
        }
        // arg[2]: local server
        RaftMessage.Server localServer = parseServer(args[2]);

        // init RPCServer
        RPCServer server = new RPCServer(localServer.getEndPoint().getPort());
        // set options
        // just for test snapshot
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setDataDir(dataPath);
        raftOptions.setSnapshotMinLogSize(10 * 1024);
        raftOptions.setSnapshotPeriodSeconds(30);
        raftOptions.setMaxSegmentFileSize(1024 * 1024);
        // application state machine
        ExampleStateMachine stateMachine = new ExampleStateMachine(raftOptions.getDataDir());
        // init RaftNode, [options, servers, localServer, stateMachine
        RaftNode raftNode = new RaftNode(raftOptions, serverList, localServer, stateMachine);

        // register
        // register rpc between raft nodes
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);
        // register for raft client
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);
        // register for self
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);

        // start server, start netty server
        server.start();
        // init raft node
        raftNode.init();
    }

    private static RaftMessage.Server parseServer(String serverString) {
        String[] splitServer = serverString.split(":");
        // host, port, id
        String host = splitServer[0];
        Integer port = Integer.parseInt(splitServer[1]);
        Integer serverId = Integer.parseInt(splitServer[2]);
        RaftMessage.EndPoint endPoint = RaftMessage.EndPoint
                                            .newBuilder()
                                            .setHost(host)
                                            .setPort(port)
                                            .build();
        RaftMessage.Server.Builder serverBuilder = RaftMessage.Server.newBuilder();
        RaftMessage.Server server = serverBuilder.setServerId(serverId).setEndPoint(endPoint).build();
        return server;
    }
}
