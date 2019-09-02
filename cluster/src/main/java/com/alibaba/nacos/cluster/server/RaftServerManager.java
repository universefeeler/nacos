package com.alibaba.nacos.cluster.server;

import com.alibaba.nacos.cluster.server.jraft.JRaftServerImpl;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.misc.NetUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @Author: wanglei1
 * @Date: 2019/08/27 18:21
 * @Description:
 */
@Component("raftServerManager")
public class RaftServerManager {

    @Autowired
    private ServerListManager serverListManager;

    @PostConstruct
    public void init() {

        List<Server> refreshedServers = serverListManager.refreshServerList();

        Server server = new Server();
        server.setIp(NetUtils.getLocalAddress());
        server.setServePort(RunningConfig.getServerPort());

        JRaftServerImpl.getInstance().init(server, refreshedServers);
    }

}
