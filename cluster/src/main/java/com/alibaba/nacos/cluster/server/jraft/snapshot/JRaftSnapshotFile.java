package com.alibaba.nacos.cluster.server.jraft.snapshot;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.cluster.constant.ClusterConstant;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DataStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;

/**
 * @Author: wanglei1
 * @Date: 2019/08/01 16:10
 * @Description:
 */
public class JRaftSnapshotFile {

    private String              path;

    public JRaftSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    public boolean save(DataStore dataStore) {
        try {
            FileUtils.writeStringToFile(new File(path), JSONObject.toJSONString(dataStore));
            return true;
        } catch (IOException e) {
            ClusterConstant.CLUSTER_LOGGER.error("Fail to save snapshot", e);
            return false;
        }
    }

    public DataStore load() throws IOException {
        String s = FileUtils.readFileToString(new File(path));
        if (!StringUtils.isBlank(s)) {
            return JSONObject.parseObject(s, DataStore.class);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + s);
    }
}
