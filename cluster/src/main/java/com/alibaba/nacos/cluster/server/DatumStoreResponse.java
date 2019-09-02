package com.alibaba.nacos.cluster.server;

import com.alibaba.nacos.naming.consistency.Datum;

import java.util.Map;
import java.util.Set;

/**
 * @Author: wanglei1
 * @Date: 2019/08/27 11:16
 * @Description:
 */
public class DatumStoreResponse extends BaseResponse<byte[]>  {

    private static final long serialVersionUID = 7069873429245050108L;

    private Map<String, Datum> subDataMap;

    private Map<String, Datum> allDataMap;

    private String key;

    private Datum value;

    private int instanceCount;

    private Datum datum;

    private Set<String> keySet;

    private boolean containsFlag;

    public Map<String, Datum> getSubDataMap() {
        return subDataMap;
    }

    public void setSubDataMap(Map<String, Datum> subDataMap) {
        this.subDataMap = subDataMap;
    }

    public Map<String, Datum> getAllDataMap() {
        return allDataMap;
    }

    public void setAllDataMap(Map<String, Datum> allDataMap) {
        this.allDataMap = allDataMap;
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

    public int getInstanceCount() {
        return instanceCount;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount = instanceCount;
    }

    public Datum getDatum() {
        return datum;
    }

    public void setDatum(Datum datum) {
        this.datum = datum;
    }

    public Set<String> getKeySet() {
        return keySet;
    }

    public void setKeySet(Set<String> keySet) {
        this.keySet = keySet;
    }

    public boolean isContainsFlag() {
        return containsFlag;
    }

    public void setContainsFlag(boolean containsFlag) {
        this.containsFlag = containsFlag;
    }

    @Override
    public String toString() {
        return "DatumStoreResponse{" +
            "subDataMap=" + subDataMap +
            ", allDataMap=" + allDataMap +
            ", key='" + key + '\'' +
            ", value=" + value +
            ", instanceCount=" + instanceCount +
            ", datum=" + datum +
            ", keySet=" + keySet +
            ", containsFlag=" + containsFlag +
            '}';
    }
}
