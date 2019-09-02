package com.alibaba.nacos.cluster.server.jraft;

/**
 * @Author: wanglei1
 * @Date: 2019/08/29 14:49
 * @Description:
 */
public interface OperationMagic {

    byte PUT = 0x01;
    byte REMOVE = 0x02;
    byte KEYS = 0x03;
    byte GET = 0x04;
    byte CONTAINS = 0x05;
    byte BATCH_GET = 0x06;
    byte GET_INSTANCE_COUNT = 0x07;
    byte GET_DATA_MAP = 0x08;

}
