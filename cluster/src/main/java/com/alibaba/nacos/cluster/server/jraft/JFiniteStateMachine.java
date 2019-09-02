package com.alibaba.nacos.cluster.server.jraft;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.cluster.server.BaseRequest;
import com.alibaba.nacos.cluster.server.DatumStoreRequest;
import com.alibaba.nacos.cluster.server.DatumStoreResponse;
import com.alibaba.nacos.cluster.server.jraft.business.*;
import com.alibaba.nacos.cluster.server.jraft.snapshot.JRaftSnapshotFile;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DataStore;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: wanglei1
 * @Date: 2019/07/30 22:02
 * @Description:
 */
public class JFiniteStateMachine extends StateMachineAdapter {

    private final AtomicLong  leaderTerm = new AtomicLong(-1);

    public DataStore getDataStore() {
        return dataStore;
    }

    public void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    private DataStore dataStore = new DataStore();

    @Override
    public void onApply(Iterator iterator) {
        //traverse log
        while (iterator.hasNext()) {
            byte magic = 0;
            BaseRequest request = null;
            RequestProcessClosure closure = null;
            //call after apply log,if not null,then means leader
            if (iterator.done() != null) {
                // it is leader, directly operator, avoid unserviable
                closure = (RequestProcessClosure) iterator.done();
                request = closure.getRequest();
                magic = request.getMagic();
            } else {
                ByteBuffer data = iterator.getData();
                try {
                    request = SerializerManager.getSerializer(SerializerManager.Hessian2)
                        .deserialize(data.array(), BaseRequest.class.getName());
                    magic = request.getMagic();
                } catch (CodecException e) {
                    ClusterConstant.CLUSTER_LOGGER.error("Fail to decode Request", e);
                }
            }
            onApplyExecute(magic, request, closure);
            ClusterConstant.CLUSTER_LOGGER.info("execute magic={} at logIndex={}", magic, iterator.getIndex());
            iterator.next();
        }
    }

    public void onApplyExecute(byte magic, BaseRequest request, RequestProcessClosure closure) {
        DatumStoreRequest datumStoreRequest = (DatumStoreRequest) request;
        DatumStoreResponse datumStoreResponse = new DatumStoreResponse();
        datumStoreResponse.setSuccess(true);
        try {
            switch (magic){
                case 0x01:
                    this.dataStore.put(datumStoreRequest.getKey(), datumStoreRequest.getValue());
                    break;
                case 0x02:
                    this.dataStore.remove(datumStoreRequest.getKey());
                    break;
                case 0x03:
                    Set<String> keySet =  this.dataStore.keys();
                    datumStoreResponse.setKeySet(keySet);
                    break;
                case 0x04:
                    Datum datum = this.dataStore.get(datumStoreRequest.getKey());
                    datumStoreResponse.setDatum(datum);
                    break;
                case 0x05:
                    boolean containsFlag = this.dataStore.contains(datumStoreRequest.getKey());
                    datumStoreResponse.setContainsFlag(containsFlag);
                    break;
                case 0x06:
                    Map<String, Datum> subMap = this.dataStore.batchGet(datumStoreRequest.getKeys());
                    datumStoreResponse.setSubDataMap(subMap);
                    break;
                case 0x07:
                    int instanceCount = this.dataStore.getInstanceCount();
                    datumStoreResponse.setInstanceCount(instanceCount);
                    break;
                case 0x08:
                    Map<String, Datum> allDataMap = this.dataStore.getDataMap();
                    datumStoreResponse.setAllDataMap(allDataMap);
                    break;
                default:
                    datumStoreResponse.setSuccess(false);
                    datumStoreResponse.setErrorMsg("unkown magic type");
                    break;
            }
            if (closure != null) {
                closure.setResponse(datumStoreResponse);
                closure.run(Status.OK());
            }
        } catch (Exception e) {
            datumStoreResponse.setSuccess(false);
            datumStoreResponse.setErrorMsg("onApplyExecute have exception "+e.getMessage());
            if (closure != null) {
                closure.setResponse(datumStoreResponse);
                closure.run(Status.OK());
            }
            ClusterConstant.CLUSTER_LOGGER.error("Fail to onApplyExecute have exception:{}", e);
        }
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        // leader no need from snapshot, leader not accept snapshot request
        if (isLeader()) {
            ClusterConstant.CLUSTER_LOGGER.error("Leader is not supposed to load snapshot");
            return false;
        }
        if (null == reader.getFileMeta("data")) {
            ClusterConstant.CLUSTER_LOGGER.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        //save snapshot to reader.getPath()/data
        JRaftSnapshotFile snapshot = new JRaftSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.setDataStore(snapshot.load());
            return true;
        } catch (IOException e) {
            ClusterConstant.CLUSTER_LOGGER.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {

        final DataStore currDataStore = this.getDataStore();
        Utils.runInThread(new Runnable() {
            @Override
            public void run() {
                JRaftSnapshotFile snapshot = new JRaftSnapshotFile(writer.getPath() + File.separator + "data");
                if (snapshot.save(currDataStore)) {
                    if (writer.addFile("data")) {
                        done.run(Status.OK());
                    } else {
                        done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                    }
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
                }
            }
        });
    }
}
