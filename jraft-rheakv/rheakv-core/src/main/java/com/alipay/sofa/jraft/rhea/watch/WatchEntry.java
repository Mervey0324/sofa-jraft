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
package com.alipay.sofa.jraft.rhea.watch;

import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.watch.WatchListener;
import com.alipay.sofa.jraft.util.BytesUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 *
 * @author jiachun.fjc
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WatchEntry implements Serializable {

    private static final long serialVersionUID = -5678680976506834026L;

    private byte[]            key;
    private WatchListener     listener;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public WatchListener getListener() {
        return listener;
    }

    public void setListener(WatchListener listener) {
        this.listener = listener;
    }

    public int length() {
        return (this.key == null ? 0 : this.key.length)
               + (this.listener == null ? 0 : Serializers.getDefault().writeObject(this.listener).length);
    }

    @Override
    public String toString() {
        return "KVEntry{" + "key=" + BytesUtil.toHex(key) + "}";
    }
}