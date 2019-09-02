package com.alibaba.nacos.cluster.server.jraft;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.cluster.server.*;
import com.alibaba.nacos.cluster.server.jraft.business.*;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.rpc.RpcServer;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wanglei1
 * @Date: 2019/07/30 19:30
 * @Description:
 */
public class JRaftServerImpl implements IRaftServer {

    private final JFiniteStateMachine jFiniteStateMachine = new JFiniteStateMachine();
    private final TimerManager timerManager = new TimerManager();

    private JRaftServerImpl(){}

    private RpcServer rpcServer;

    private RaftGroupService raftGroupService;

    private Node node;

    static {
        try {
            //init path
            FileUtils.forceMkdir(new File(ClusterConstant.CLUSTER_DATA_PATH));
        } catch (IOException e) {
            ClusterConstant.CLUSTER_LOGGER.error("JRaftServerImpl#init forceMkdir with storePath:{} have exception:{}", ClusterConstant.CLUSTER_DATA_PATH, e);
        }
    }

    @Override
    public void init(Server localNode, List<Server> nodeList) {

        List<PeerId> list = new ArrayList<>();
        for(Server serverNode: nodeList) {
            PeerId peerId = PeerId.parsePeer(serverNode.getKey());
            list.add(peerId);
        }
        Configuration initConf = new Configuration(list);
        PeerId serverId = PeerId.parsePeer(localNode.getKey());

        init(serverId, initConf);
    }

    public void init(PeerId serverId, Configuration initConf) {

        timerManager.init(50);
        rpcServer = new RpcServer(serverId.getPort());
        RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);
        rpcServer.registerUserProcessor(new DatumStoreProcessor(this));

        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setInitialConf(initConf);
        nodeOptions.setElectionTimeoutMs(5000);
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(30);
        nodeOptions.setFsm(this.jFiniteStateMachine);
        nodeOptions.setLogUri(ClusterConstant.CLUSTER_DATA_PATH + File.separator + "log");
        nodeOptions.setRaftMetaUri(ClusterConstant.CLUSTER_DATA_PATH + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(ClusterConstant.CLUSTER_DATA_PATH + File.separator + "snapshot");

        this.raftGroupService = new RaftGroupService(ClusterConstant.CLUSTER_GROUP_ID, serverId, nodeOptions, rpcServer);

        this.node = this.raftGroupService.start();

        ClusterConstant.CLUSTER_LOGGER.info("Started JRaftServerImpl server at port:{}", this.getNode().getNodeId().getPeerId().getPort());
    }

    public JFiniteStateMachine getJFiniteStateMachine() {
        return this.jFiniteStateMachine;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }


    public BaseResponse redirect(BaseResponse response) {
        response.setSuccess(false);
        if (node != null) {
            PeerId leader = node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    public void handleRequest(DatumStoreRequest request, RequestProcessClosure<BaseRequest,BaseResponse<?>> closure) {
        final DatumStoreResponse response = new DatumStoreResponse();
        // if it is not leader,then direct
        if (!this.getNode().isLeader()) {
            closure.getAsyncContext().sendResponse(this.redirect(response));
            return;
        }
        try {
            //build task
            Task task = new Task();
            //set callback
            task.setDone(closure);
            //set data
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));
            //commit to raft group
            this.getNode().apply(task);
        } catch (CodecException e) {
            ClusterConstant.CLUSTER_LOGGER.error("Fail to encode DatumStoreRequest", e);
            response.setSuccess(false);
            response.setErrorMsg(e.getMessage());
            closure.getAsyncContext().sendResponse(response);
        }
    }

    private static class SingleTonHoler{
        private static final IRaftServer INSTANCE = new JRaftServerImpl();
    }

    public static IRaftServer getInstance(){
        return SingleTonHoler.INSTANCE;
    }
}
