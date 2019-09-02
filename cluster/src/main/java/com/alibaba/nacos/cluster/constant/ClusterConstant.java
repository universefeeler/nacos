package com.alibaba.nacos.cluster.constant;

import com.alibaba.nacos.core.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @Author: wanglei1
 * @Date: 2019/07/30 19:43
 * @Description:
 */
public interface ClusterConstant {

    String CLUSTER_PROTOCL = "raft";

    String CLUSTER_PATH = SystemUtils.NACOS_HOME + File.separator + CLUSTER_PROTOCL;

    String CLUSTER_DATA_PATH = CLUSTER_PATH + File.separator + "data";

    String CLUSTER_GROUP_ID = "nacos_raft";

    Logger CLUSTER_LOGGER = LoggerFactory.getLogger("com.alibaba.nacos.cluster");
}
