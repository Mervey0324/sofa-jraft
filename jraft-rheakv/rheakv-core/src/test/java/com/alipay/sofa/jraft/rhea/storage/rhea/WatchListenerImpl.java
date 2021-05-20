package com.alipay.sofa.jraft.rhea.storage.rhea;

import com.alipay.sofa.jraft.rhea.watch.WatchEvent;
import com.alipay.sofa.jraft.rhea.watch.WatchListener;
import com.alipay.sofa.jraft.util.BytesUtil;

public class WatchListenerImpl implements WatchListener {

    @Override
    public void onNext(WatchEvent event) {
        String msg = ">>>>>>>>>>>>>>> watch listener onNext is called! "
                + "\nkey is " + BytesUtil.readUtf8(event.getKey())
                + "\npreValue is " + event.getPreValue() == null?"null":BytesUtil.readUtf8(event.getPreValue())
                + "\ncurValue is " + event.getValue() == null?"null":BytesUtil.readUtf8(event.getValue())
                + "\nevent type is " + event.getEventType().name();
        System.out.println(msg);
    }

    @Override
    public void onError(Throwable throwable) {
        String msg = ">>>>>>>>>>>>>>> watch listener onError is called! "
                + "\nerror is " + throwable.toString();
        System.out.println(msg);
    }
}
