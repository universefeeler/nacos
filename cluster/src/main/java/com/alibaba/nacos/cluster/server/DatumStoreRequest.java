package com.alibaba.nacos.cluster.server;

import com.alibaba.nacos.naming.consistency.Datum;

import java.util.List;

/**
 * @Author: wanglei1
 * @Date: 2019/08/27 11:16
 * @Description:
 */
public class DatumStoreRequest extends BaseRequest {

    private static final long serialVersionUID = -8796389415028072929L;

    private List<String> keys;

    private String key;

    private Datum value;

    private boolean readOnlySafe     = true;

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Datum getValue() {
        return value;
    }

    public void setValue(Datum value) {
        this.value = value;
    }

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }

    @Override
    public String toString() {
        return "DatumStoreRequest{" +
            "keys=" + keys +
            ", key='" + key + '\'' +
            ", value=" + value +
            ", readOnlySafe=" + readOnlySafe +
            '}';
    }
}
