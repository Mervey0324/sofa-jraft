package com.alipay.sofa.jraft.rhea.storage.rhea;

import com.alipay.sofa.jraft.rhea.watch.WatchEvent;
import com.alipay.sofa.jraft.rhea.watch.WatchListener;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchListenerImpl implements WatchListener {
    private static final Logger LOG = LoggerFactory.getLogger(WatchListenerImpl.class);

    @Override
    public void onNext(WatchEvent event) {
        LOG.info(">>>>>>>>> watch listener onNext is called! key is {}, preValue is {}, curValue is {}, event type is {}",
                BytesUtil.readUtf8(event.getKey()),
                event.getPreValue() == null?"null":BytesUtil.readUtf8(event.getPreValue()),
                event.getValue() == null?"null":BytesUtil.readUtf8(event.getValue()),
                event.getEventType().name()
        );
    }

    @Override
    public void onError(Throwable throwable) {
        String msg = ">>>>>>>>>>>>>>> watch listener onError is called! "
                + "\nerror is " + throwable.toString();
        System.out.println(msg);
    }
}
