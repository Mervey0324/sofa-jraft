/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        LOG.info(
            ">>>>>>>>> watch listener onNext is called! key is {}, preValue is {}, curValue is {}, event type is {}",
            BytesUtil.readUtf8(event.getKey()),
            event.getPreValue() == null ? "null" : BytesUtil.readUtf8(event.getPreValue()),
            event.getValue() == null ? "null" : BytesUtil.readUtf8(event.getValue()), event.getEventType().name());
    }

    @Override
    public void onError(Throwable throwable) {
        String msg = ">>>>>>>>>>>>>>> watch listener onError is called! " + "\nerror is " + throwable.toString();
        System.out.println(msg);
    }
}
