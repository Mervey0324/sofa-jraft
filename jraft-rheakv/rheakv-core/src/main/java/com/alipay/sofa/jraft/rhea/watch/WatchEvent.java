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

import com.alipay.sofa.jraft.util.BytesUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WatchEvent {
    private byte[]         key;
    private byte[]         preValue;
    private byte[]         value;
    private EventType      eventType;
    private CountDownLatch shutdown;

    public WatchEvent(byte[] key, byte[] preValue, byte[] value, EventType eventType) {
        this.key = key;
        this.preValue = preValue;
        this.value = value;
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "WatchEvent{" + "key=" + BytesUtil.readUtf8(key) + ", preValue=" + preValue == null ? "null" : BytesUtil
            .readUtf8(preValue) + ", value=" + value == null ? "null" : BytesUtil.readUtf8(value) + ", eventType="
                                                                        + eventType + '}';
    }
}
