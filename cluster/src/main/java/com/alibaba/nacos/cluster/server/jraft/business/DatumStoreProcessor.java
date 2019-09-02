package com.alibaba.nacos.cluster.server.jraft.business;

import com.alibaba.nacos.cluster.server.BaseRequest;
import com.alibaba.nacos.cluster.server.BaseResponse;
import com.alibaba.nacos.cluster.server.DatumStoreRequest;
import com.alibaba.nacos.cluster.server.jraft.JRaftServerImpl;
import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;

/**
 * @Author: wanglei1
 * @Date: 2019/08/01 1:39
 * @Description:
 */
public class DatumStoreProcessor extends AsyncUserProcessor<DatumStoreRequest> {

    private JRaftServerImpl jRaftServerImpl;

    public DatumStoreProcessor(JRaftServerImpl jRaftServerImpl) {
        super();
        this.jRaftServerImpl = jRaftServerImpl;
    }

    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final DatumStoreRequest request) {
        final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure = new RequestProcessClosure<>(request,
            bizCtx, asyncCtx);
        jRaftServerImpl.handleRequest(request, closure);
    }

    @Override
    public String interest() {
        return DatumStoreRequest.class.getName();
    }
}
