package com.alibaba.nacos.cluster.server;

import com.alibaba.nacos.naming.cluster.servers.Server;

import java.util.List;

/**
 * @Author: wanglei1
 * @Date: 2019/07/30 19:25
 * @Description:
 */
public interface IRaftServer {


    /**
     * init raft server
     * @param localServerNode
     * @param nodeList
     */
    void init(Server localServerNode, List<Server> nodeList);


}
