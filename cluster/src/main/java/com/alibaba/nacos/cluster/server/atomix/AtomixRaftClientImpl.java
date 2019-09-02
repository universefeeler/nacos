package com.alibaba.nacos.cluster.server.atomix;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.cluster.server.IRaftClient;
import com.alibaba.nacos.cluster.server.atomix.protocol.LocalRaftProtocolFactory;
import com.alibaba.nacos.cluster.server.atomix.protocol.RaftClientMessagingProtocol;
import com.alibaba.nacos.cluster.utils.IPUtil;
import com.alibaba.nacos.naming.cluster.servers.Server;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.protocol.*;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import io.atomix.protocols.raft.storage.log.entry.*;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.atomix.primitive.operation.PrimitiveOperation.operation;

/**
 * @Author: wanglei1
 * @Date: 2019/08/28 16:30
 * @Description:
 */
public class AtomixRaftClientImpl implements IRaftClient {

    private static final Serializer SERIALIZER = Serializer.using(AtomixPrimitiveType.INSTANCE.namespace());

    private static final boolean USE_NETTY = true;

    private Map<MemberId, Address> addressMap = new ConcurrentHashMap<>();

    private LocalRaftProtocolFactory protocolFactory = new LocalRaftProtocolFactory(PROTOCOL_SERIALIZER);

    @Override
    public void init(Server localServerNode, List<Server> nodeList) {

        Member member = nextNode(localServerNode.getIp(), localServerNode.getServePort(),
            String.valueOf(IPUtil.ipToLong(localServerNode.getIp())+localServerNode.getServePort()));
        List<Member> nodes = new ArrayList<>();
        for(Server server: nodeList) {
            nodes.add(nextNode(server.getIp(), server.getServePort(),
                String.valueOf(IPUtil.ipToLong(server.getIp()) + server.getServePort())));
        }
        RaftClient raftClient = initClient(member, nodes);

        CompletableFuture<Void> future = new CompletableFuture();
        SessionClient proxy = createProxy(raftClient).connect().join();
        Thread t = new Thread(new ClientThread(future, raftClient, proxy));
        t.setName("AtomixRaftClientThread");
        t.start();

        CompletableFuture.allOf(future).join();
    }

    private RaftClient initClient(Member member, List<Member> nodes) {

        RaftClientProtocol protocol;
        if (USE_NETTY) {
            MessagingService messagingManager = new NettyMessagingService(ClusterConstant.CLUSTER_GROUP_ID, member.address(), new MessagingConfig()).start().join();
            addressMap.put(member.id(), member.address());
            protocol = new RaftClientMessagingProtocol(messagingManager, PROTOCOL_SERIALIZER, addressMap::get);
        } else {
            protocol = protocolFactory.newClientProtocol(member.id());
        }

        RaftClient client = RaftClient.builder()
            .withMemberId(member.id())
            .withProtocol(protocol)
            .withPartitionId(PartitionId.from(ClusterConstant.CLUSTER_GROUP_ID, 1))
            .build();
        client.connect(nodes.stream().map(Member::id).collect(Collectors.toList())).join();
        return client;
    }

    private Member nextNode(String ip, int port, String id) {
        Address address = Address.from(ip, ++port);
        Member member = Member.builder(MemberId.from(String.valueOf(id)))
            .withAddress(address)
            .build();
        return member;
    }

    private SessionClient createProxy(RaftClient client) {
        return client.sessionBuilder(ClusterConstant.CLUSTER_GROUP_ID, AtomicCounterType.instance(), new ServiceConfig())
            .withReadConsistency(READ_CONSISTENCY)
            .withCommunicationStrategy(COMMUNICATION_STRATEGY)
            .build();
    }

    class ClientThread implements Runnable {

        CompletableFuture<Void> future;
        RaftClient client;
        SessionClient proxy;

        public ClientThread(CompletableFuture<Void> future, RaftClient client, SessionClient proxy) {
            this.future = future;
            this.client = client;
            this.proxy = proxy;
        }

        @Override
        public void run() {
            ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r);
                t.setName("atomix.nacos");
                t.setDaemon(true);
                return t;
            });
            executor.scheduleAtFixedRate(() -> {
                try {
                    runProxy(proxy, future);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 0, 10, TimeUnit.MILLISECONDS);
        }

        private void runProxy(SessionClient proxy, CompletableFuture<Void> future) {
            proxy.execute(operation(OperationCmd.GET, SERIALIZER.encode(null)))
                .<Long>thenApply(SERIALIZER::decode)
                .thenAccept(result -> {
                    System.out.println("this is current: "+ result);
                });
        }
    }

    private static final ReadConsistency READ_CONSISTENCY = ReadConsistency.LINEARIZABLE;
    private static final CommunicationStrategy COMMUNICATION_STRATEGY = CommunicationStrategy.ANY;
    private static final Serializer PROTOCOL_SERIALIZER = Serializer.using(Namespace.builder()
        .register(HeartbeatRequest.class)
        .register(HeartbeatResponse.class)
        .register(OpenSessionRequest.class)
        .register(OpenSessionResponse.class)
        .register(CloseSessionRequest.class)
        .register(CloseSessionResponse.class)
        .register(KeepAliveRequest.class)
        .register(KeepAliveResponse.class)
        .register(QueryRequest.class)
        .register(QueryResponse.class)
        .register(CommandRequest.class)
        .register(CommandResponse.class)
        .register(MetadataRequest.class)
        .register(MetadataResponse.class)
        .register(JoinRequest.class)
        .register(JoinResponse.class)
        .register(LeaveRequest.class)
        .register(LeaveResponse.class)
        .register(ConfigureRequest.class)
        .register(ConfigureResponse.class)
        .register(ReconfigureRequest.class)
        .register(ReconfigureResponse.class)
        .register(InstallRequest.class)
        .register(InstallResponse.class)
        .register(PollRequest.class)
        .register(PollResponse.class)
        .register(VoteRequest.class)
        .register(VoteResponse.class)
        .register(AppendRequest.class)
        .register(AppendResponse.class)
        .register(PublishRequest.class)
        .register(ResetRequest.class)
        .register(RaftResponse.Status.class)
        .register(RaftError.class)
        .register(RaftError.Type.class)
        .register(PrimitiveOperation.class)
        .register(ReadConsistency.class)
        .register(byte[].class)
        .register(long[].class)
        .register(CloseSessionEntry.class)
        .register(CommandEntry.class)
        .register(ConfigurationEntry.class)
        .register(InitializeEntry.class)
        .register(KeepAliveEntry.class)
        .register(MetadataEntry.class)
        .register(OpenSessionEntry.class)
        .register(QueryEntry.class)
        .register(PrimitiveOperation.class)
        .register(DefaultOperationId.class)
        .register(OperationType.class)
        .register(ReadConsistency.class)
        .register(ArrayList.class)
        .register(Collections.emptyList().getClass())
        .register(HashSet.class)
        .register(DefaultRaftMember.class)
        .register(MemberId.class)
        .register(SessionId.class)
        .register(RaftMember.Type.class)
        .register(Instant.class)
        .register(Configuration.class)
        .build());
}
