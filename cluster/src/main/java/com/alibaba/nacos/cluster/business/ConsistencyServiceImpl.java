package com.alibaba.nacos.cluster.business;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.pojo.Record;

/**
 * @Author: wanglei1
 * @Date: 2019/07/31 1:14
 * @Description:
 */
public class ConsistencyServiceImpl implements ConsistencyService {

    @Override
    public void put(String key, Record value) throws NacosException {

    }

    @Override
    public void remove(String key) throws NacosException {

    }

    @Override
    public Datum get(String key) throws NacosException {
        return null;
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {

    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {

    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
