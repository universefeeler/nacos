package com.alibaba.nacos.cluster.server.atomix;

import com.alibaba.nacos.cluster.constant.ClusterConstant;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

/**
 * @Author: wanglei1
 * @Date: 2019/08/28 19:08
 * @Description:
 */
public class AtomixPrimitiveType implements PrimitiveType {

    public static final AtomixPrimitiveType INSTANCE = new AtomixPrimitiveType();

    @Override
    public String name() {
        return ClusterConstant.CLUSTER_GROUP_ID;
    }

    @Override
    public PrimitiveConfig newConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveBuilder newBuilder(String primitiveName, PrimitiveConfig config, PrimitiveManagementService managementService) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveService newService(ServiceConfig config) {
        return new AtomixStateMachine();
    }
}
