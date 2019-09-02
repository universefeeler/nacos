package com.alibaba.nacos.cluster.server.atomix;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.cluster.server.IRaftServer;
import com.alibaba.nacos.cluster.server.atomix.protocol.LocalRaftProtocolFactory;
import com.alibaba.nacos.cluster.server.atomix.protocol.RaftServerMessagingProtocol;
import com.alibaba.nacos.cluster.utils.IPUtil;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.impl.DefaultClusterMembershipService;
import io.atomix.cluster.impl.DefaultNodeDiscoveryService;
import io.atomix.cluster.messaging.*;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocol;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocolConfig;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.protocol.*;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.entry.*;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.Version;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @Author: wanglei1
 * @Date: 2019/08/28 11:49
 * @Description:
 */
public class AtomixRaftServerImpl implements IRaftServer {

    private static final boolean USE_NETTY = true;
    private RaftServerProtocol protocol;
    private LocalRaftProtocolFactory protocolFactory = new LocalRaftProtocolFactory(PROTOCOL_SERIALIZER);
    private ManagedMessagingService messagingService;
    private List<ManagedMessagingService> messagingServices = new ArrayList<>();

    private Map<MemberId, Address> addressMap = new ConcurrentHashMap<>();

    private AtomixRaftServerImpl(){}

    @Override
    public void init(Server localServerNode, List<Server> nodeList) {

        Member member = nextNode(localServerNode.getIp(), localServerNode.getServePort(),
            String.valueOf(IPUtil.ipToLong(localServerNode.getIp())+localServerNode.getServePort()));
        List<Member> nodes = new ArrayList<>();
        for(Server server: nodeList) {
            nodes.add(nextNode(server.getIp(), server.getServePort(),
                String.valueOf(IPUtil.ipToLong(server.getIp()) + server.getServePort())));
        }
        initServer(member, Lists.newArrayList(nodes));
    }

    public RaftServer initServer(Member member, List<Node> nodes) {
        if (USE_NETTY) {
            messagingService = (ManagedMessagingService) new NettyMessagingService(ClusterConstant.CLUSTER_GROUP_ID, member.address(), new MessagingConfig())
                .start()
                .join();
            messagingServices.add(messagingService);
            protocol = new RaftServerMessagingProtocol(messagingService, PROTOCOL_SERIALIZER, addressMap::get);
        } else {
            protocol = protocolFactory.newServerProtocol(member.id());
        }

        BootstrapService bootstrapService = new BootstrapService() {
            @Override
            public MessagingService getMessagingService() {
                return messagingService;
            }

            @Override
            public UnicastService getUnicastService() {
                return new UnicastServiceAdapter();
            }

            @Override
            public BroadcastService getBroadcastService() {
                return new BroadcastServiceAdapter();
            }
        };

        List<PrimitiveType> list = new ArrayList<>();
        list.add(AtomicCounterType.instance());

        RaftServer.Builder builder = RaftServer.builder(member.id())
            .withProtocol(protocol)
            .withPrimitiveTypes(new DefaultPrimitiveTypeRegistry(list))
            .withThreadModel(ThreadModel.SHARED_THREAD_POOL)
            .withMembershipService(new DefaultClusterMembershipService(
                member,
                Version.from("1.0.0"),
                new DefaultNodeDiscoveryService(bootstrapService, member, new BootstrapDiscoveryProvider(nodes)),
                bootstrapService,
                new HeartbeatMembershipProtocol(new HeartbeatMembershipProtocolConfig())))
                .withStorage(RaftStorage.builder()
                .withStorageLevel(StorageLevel.DISK)
                .withDirectory(new File(String.format("target/perf-logs/%s", member.id())))
                .withNamespace(STORAGE_NAMESPACE)
                .withMaxSegmentSize(1024 * 1024 * 64)
                .withDynamicCompaction()
                .withFlushOnCommit(false)
                .build());

        RaftServer server = builder.build();
        return server;
    }


    private static class SingleTonHoler{
        private static final IRaftServer INSTANCE = new AtomixRaftServerImpl();
    }

    public static IRaftServer getInstance(){
        return AtomixRaftServerImpl.SingleTonHoler.INSTANCE;
    }

    private Member nextNode(String ip, int port, String id) {
        Address address = Address.from(ip, ++port);
        Member member = Member.builder(MemberId.from(String.valueOf(id)))
            .withAddress(address)
            .build();
        return member;
    }

    private static class UnicastServiceAdapter implements UnicastService {
        @Override
        public void unicast(Address address, String subject, byte[] message) {

        }

        @Override
        public void addListener(String subject, BiConsumer<Address, byte[]> listener, Executor executor) {

        }

        @Override
        public void removeListener(String subject, BiConsumer<Address, byte[]> listener) {

        }
    }

    private static class BroadcastServiceAdapter implements BroadcastService {
        @Override
        public void broadcast(String subject, byte[] message) {

        }

        @Override
        public void addListener(String subject, Consumer<byte[]> listener) {

        }

        @Override
        public void removeListener(String subject, Consumer<byte[]> listener) {

        }
    }

    private static final ReadConsistency READ_CONSISTENCY = ReadConsistency.LINEARIZABLE;

    private static final CommunicationStrategy COMMUNICATION_STRATEGY = CommunicationStrategy.ANY;

    private static final Serializer CLIENT_SERIALIZER = Serializer.using(Namespace.builder()
        .register(ReadConsistency.class)
        .register(Maps.immutableEntry("", "").getClass())
        .build());

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
        .register(io.atomix.protocols.raft.storage.system.Configuration.class)
        .build());

    private static final Namespace STORAGE_NAMESPACE = Namespace.builder()
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
        .register(HashSet.class)
        .register(DefaultRaftMember.class)
        .register(MemberId.class)
        .register(RaftMember.Type.class)
        .register(Instant.class)
        .register(io.atomix.protocols.raft.storage.system.Configuration.class)
        .register(byte[].class)
        .register(long[].class)
        .build();
}
