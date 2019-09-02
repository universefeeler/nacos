package com.alibaba.nacos.cluster.server.jraft;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.cluster.server.IRaftClient;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @Author: wanglei1
 * @Date: 2019/07/31 0:05
 * @Description:
 */
public class JRaftClientImpl implements IRaftClient {

    private  BoltCliClientService boltCliClientService;

    private RouteTable routeTable;

    @Override
    public void init(Server server, List<Server> nodeList) {

        List<PeerId> list = new ArrayList<>();
        for(Server serverNode: nodeList) {
            PeerId peerId = PeerId.parsePeer(serverNode.getKey());
            list.add(peerId);
        }
        Configuration initConf = new Configuration(list);
        RouteTable.getInstance().updateConfiguration(ClusterConstant.CLUSTER_GROUP_ID, initConf);
        boltCliClientService = new BoltCliClientService();
        boltCliClientService.init(new CliOptions());
        try {
            RouteTable.getInstance().refreshLeader(boltCliClientService, ClusterConstant.CLUSTER_GROUP_ID, 1000).isOk();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        this.routeTable = RouteTable.getInstance();
    }

    private static class SingleTonHoler{
        private static final JRaftClientImpl INSTANCE = new JRaftClientImpl();
    }

    public static JRaftClientImpl getInstance(){
        return JRaftClientImpl.SingleTonHoler.INSTANCE;
    }

    public BoltCliClientService getBoltCliClientService() {
        return boltCliClientService;
    }

    public void setBoltCliClientService(BoltCliClientService boltCliClientService) {
        this.boltCliClientService = boltCliClientService;
    }

    public RouteTable getRouteTable() {
        return routeTable;
    }

    public void setRouteTable(RouteTable routeTable) {
        this.routeTable = routeTable;
    }
}
