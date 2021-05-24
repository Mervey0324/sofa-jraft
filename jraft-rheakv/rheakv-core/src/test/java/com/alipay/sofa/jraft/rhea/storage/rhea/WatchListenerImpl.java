package com.alipay.sofa.jraft.rhea.storage.rhea;

import com.alipay.sofa.jraft.rhea.watch.WatchEvent;
import com.alipay.sofa.jraft.rhea.watch.WatchListener;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class WatchListenerImpl implements WatchListener {
    private static final Logger LOG = LoggerFactory.getLogger(WatchListenerImpl.class);
    private final AtomicInteger nextCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);

    @Override
    public void onNext(WatchEvent event) {
        LOG.info(">>>>>>>>> watch listener onNext is called! key is {}, preValue is {}, curValue is {}, event type is {}",
                BytesUtil.readUtf8(event.getKey()),
                event.getPreValue() == null?"null":BytesUtil.readUtf8(event.getPreValue()),
                event.getValue() == null?"null":BytesUtil.readUtf8(event.getValue()),
                event.getEventType().name()
        );
        nextCount.incrementAndGet();
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.warn(">>>>>>>>>>>>>>> watch listener onError is called!", throwable);
        errorCount.incrementAndGet();
    }

    @Override
    public String toString() {
        return "WatchListenerImpl{" +
                "nextCount=" + nextCount.get() +
                ", errorCount=" + errorCount.get() +
                '}';
    }
}
