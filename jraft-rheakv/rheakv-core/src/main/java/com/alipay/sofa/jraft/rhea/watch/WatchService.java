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

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.options.WatchOptions;

import java.util.List;
import java.util.Set;

public interface WatchService extends Lifecycle<WatchOptions> {
    // listener api
    void addListener(byte[] key, WatchListener listener, boolean prefix);
    void addListeners(List<AddListener> listeners);
    void removeListener(byte[] key);
    void removeListeners();

    // append event
    void appendEvent(WatchEvent event);
    void appendEvents(List<WatchEvent> events);

    // snapshot api
    void writeSnapshot(String snapshotPath, String suffix) throws Exception;
    void readSnapshot(String snapshotPath, String suffix) throws Exception;

    // watch
    boolean isWatched(byte[] key);
    Set<byte[]> getWatchedKeys(List<byte[]> keys);

    // other
    void join() throws InterruptedException;
}
