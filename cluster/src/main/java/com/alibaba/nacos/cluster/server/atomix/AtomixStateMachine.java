package com.alibaba.nacos.cluster.server.atomix;

import com.alibaba.nacos.cluster.server.DatumStoreRequest;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DataStore;
import io.atomix.primitive.service.*;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.Map;


/**
 * @Author: wanglei1
 * @Date: 2019/08/28 19:06
 * @Description:
 */
public class AtomixStateMachine extends AbstractPrimitiveService {

    private static final Serializer SERIALIZER = Serializer.using(AtomixPrimitiveType.INSTANCE.namespace());

    private DataStore dataStore = new DataStore();
    
    public AtomixStateMachine() {
        super(AtomixPrimitiveType.INSTANCE);
    }

    @Override
    public Serializer serializer() {
        return SERIALIZER;
    }

    @Override
    public void backup(BackupOutput writer) {
        writer.writeInt(dataStore.getDataMap().size());
        for (Map.Entry<String, Datum> entry : dataStore.getDataMap().entrySet()) {
            writer.writeString(entry.getKey());
            writer.writeObject(entry.getValue());
        }
    }

    @Override
    public void restore(BackupInput reader) {
        dataStore = new DataStore();
        Map<String, Datum> dataMap = new HashMap<>();
        int size = reader.readInt();
        for (int i = 0; i < size; i++) {
            String key = reader.readString();
            Datum value = reader.readObject();
            dataMap.put(key, value);
        }
        dataStore.setDataMap(dataMap);
    }

    @Override
    protected void configure(ServiceExecutor executor) {
        executor.register(OperationCmd.PUT, this::put);
        executor.register(OperationCmd.GET, this::get);
        executor.register(OperationCmd.REMOVE, this::remove);
        executor.register(OperationCmd.BATCH_GET, this::batch_get);
        executor.register(OperationCmd.KEYS, this::keys);
        executor.register(OperationCmd.CONTAINS, this::contains);
        executor.register(OperationCmd.GET_INSTANCE_COUNT, this::get_instance_count);
        executor.register(OperationCmd.GET_DATA_MAP, this::get_data_map);
    }

    protected void put(Commit<DatumStoreRequest> commit) {
        dataStore.put(commit.value().getKey(), commit.value().getValue());
    }

    protected Datum get(Commit<DatumStoreRequest> commit) {
        return dataStore.get(commit.value().getKey());
    }

    protected Datum remove(Commit<DatumStoreRequest> commit) {
        return dataStore.remove(commit.value().getKey());
    }

    protected long keys(Commit<DatumStoreRequest> commit) {
        return commit.index();
    }

    protected long batch_get(Commit<DatumStoreRequest> commit) {
        return commit.index();
    }

    protected boolean contains(Commit<DatumStoreRequest> commit) {
        return dataStore.contains(commit.value().getKey());
    }

    protected int get_instance_count(Commit<DatumStoreRequest> commit) {
        return dataStore.getInstanceCount();
    }

    protected Map<String, Datum> get_data_map(Commit<DatumStoreRequest> commit) {
        return dataStore.getDataMap();
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }
}
