package com.alibaba.nacos.cluster.server.atomix;

import io.atomix.primitive.operation.OperationId;

/**
 * @Author: wanglei1
 * @Date: 2019/08/28 18:36
 * @Description:
 */
public interface OperationCmd {

    OperationId PUT = OperationId.command("put");
    OperationId REMOVE = OperationId.command("remove");
    OperationId KEYS = OperationId.command("keys");
    OperationId GET = OperationId.query("get");
    OperationId CONTAINS = OperationId.query("contains");
    OperationId BATCH_GET = OperationId.query("batch_get");
    OperationId GET_INSTANCE_COUNT = OperationId.query("get_instance_count");
    OperationId GET_DATA_MAP = OperationId.query("get_data_map");

}
