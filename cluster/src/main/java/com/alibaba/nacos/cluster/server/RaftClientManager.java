package com.alibaba.nacos.cluster.server;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.cluster.server.jraft.JRaftClientImpl;
import com.alibaba.nacos.cluster.server.jraft.OperationMagic;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * @Author: wanglei1
 * @Date: 2019/08/29 14:53
 * @Description:
 */
@Component("raftClientManager")
public class RaftClientManager {

    @Autowired
    private ServerListManager serverListManager;

    private JRaftClientImpl raftClient;

    public void init() {
        List<Server> refreshedServers = serverListManager.refreshServerList();
        Server server = new Server();
        server.setIp(NetUtils.getLocalAddress());
        server.setServePort(RunningConfig.getServerPort());
        raftClient = JRaftClientImpl.getInstance();
        raftClient.init(server, refreshedServers);
    }

    public void put(String key, Datum value) throws RemotingException, InterruptedException {

        final DatumStoreRequest request = new DatumStoreRequest();
        request.setKey(key);
        request.setValue(value);
        request.setMagic(OperationMagic.PUT);
        raftClient.getBoltCliClientService().getRpcClient().invokeWithCallback(
            raftClient.getRouteTable().selectLeader(ClusterConstant.CLUSTER_GROUP_ID).getEndpoint().toString(),
            request,
            new InvokeCallback() {
                @Override
                public void onResponse(Object result) {
                    System.out.println("incrementAndGet result:" + result);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }
            }, 5000);
    }
}
