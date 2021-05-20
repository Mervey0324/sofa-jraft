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

import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.WatchOptions;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.util.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class WatchServiceImpl implements WatchService {

    private static final int           MAX_ADD_REQUEST_RETRY_TIMES = 3;
    private static final Logger        LOG                         = LoggerFactory.getLogger(WatchServiceImpl.class);

    private Disruptor<WatchEvent>      watchDisruptor;
    private RingBuffer<WatchEvent>     watchRingBuffer;
    private WatchOptions               options;

    private static final Comparator<byte[]>              COMPARATOR   = BytesUtil.getDefaultByteArrayComparator();
    private ConcurrentNavigableMap<byte[], WatchListener> listeners;
    private volatile CountDownLatch    shutdownLatch;

    private final Serializer           serializer                  = Serializers.getDefault();

    //    private Node                       node;
    //    private NodeMetrics                nodeMetrics;

    private static class WatchEventFactory implements EventFactory<WatchEvent> {
        @Override
        public WatchEvent newInstance() {
            return new WatchEvent();
        }
    }

    private class WatchEventHandler implements EventHandler<WatchEvent> {
        @Override
        public void onEvent(WatchEvent event, long sequence, boolean endOfBatch) throws Exception {
            LOG.info(">>>>>>>>> enter WatchEventHandler.onEvent");
            Requires.requireNonNull(listeners, "listeners");
            if (!listeners.isEmpty() && listeners.containsKey(event.getKey())) {
                listeners.get(event.getKey()).onNext(event);
                LOG.info(">>>>>>>>> execute listener onNext end.");
            }
        }
    }

    public WatchServiceImpl(WatchOptions options) {
        this.options = options;
        init(options);
    }

    public WatchServiceImpl() {
    }

    @Override
    public boolean init(WatchOptions watchOptions) {
        options = watchOptions;
        //        node = options.getNode();
        //        nodeMetrics = node.getNodeMetrics();
        listeners = new ConcurrentSkipListMap<>(COMPARATOR);
        watchDisruptor = DisruptorBuilder.<WatchEvent> newInstance() //
            .setEventFactory(new WatchEventFactory()) //
            .setRingBufferSize(this.options.getDisruptorBufferSize()) //
            .setThreadFactory(new NamedThreadFactory("JRaft-WatchService-Disruptor-", true)) //
            .setWaitStrategy(new BlockingWaitStrategy()) //
            .setProducerType(ProducerType.MULTI) //
            .build();
        watchDisruptor.handleEventsWith(new WatchEventHandler());
        watchDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        watchRingBuffer = watchDisruptor.start();

        //        if (nodeMetrics != null && nodeMetrics.getMetricRegistry() != null) {
        //            this.nodeMetrics.getMetricRegistry() //
        //                .register("jraft-watch-service-disruptor", new DisruptorMetricSet(this.watchRingBuffer));
        //        }

        return true;
    }

    @Override
    public void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        this.shutdownLatch = new CountDownLatch(1);
        Utils.runInThread(() -> this.watchRingBuffer.publishEvent((event, sequence) -> {}));
    }

    public void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
        }
        this.watchDisruptor.shutdown();
        listeners.clear();
    }

    @Override
    public void addListener(byte[] key, WatchListener listener) {
        this.listeners.put(key, listener);
    }

    @Override
    public void addListeners(Map<byte[], WatchListener> listeners) {
        this.listeners.putAll(listeners);
    }

    @Override
    public Map<byte[], WatchListener> getListeners() {
        return this.listeners;
    }

    @Override
    public void removeListener(byte[] key) {
        this.listeners.remove(key);
    }

    @Override
    public void appendEvent(WatchEvent event) {
        LOG.info("append watch event, event is {}", event);
        if (this.shutdownLatch != null) {
            IllegalStateException e = new IllegalStateException("Service already shutdown.");
            if (listeners.containsKey(event.getKey())) {
                listeners.get(event.getKey()).onError(e);
            }
//            throw e;
        }
        if(!this.listeners.containsKey(event.getKey()))
            return;
        try {
            EventTranslator<WatchEvent> translator = (newEvent, sequence) -> {
                newEvent.setEventType(event.getEventType());
                newEvent.setKey(event.getKey());
                newEvent.setPreValue(event.getPreValue());
                newEvent.setValue(event.getValue());
            };
            int retryTimes = 0;
            while (true) {
                if (this.watchRingBuffer.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_ADD_REQUEST_RETRY_TIMES) {
//                        if(nodeMetrics != null)
//                            this.nodeMetrics.recordTimes("read-index-overload-times", 1);
//                        LOG.warn("Node {} WatchServiceImpl watchRingBuffer is overload.", node.getNodeId());
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
        } catch (final Exception e) {
            if (listeners.containsKey(event.getKey())) {
                listeners.get(event.getKey()).onError(e);
            }
        }
    }

    @Override
    public void appendEvents(List<WatchEvent> events) {
        LOG.info("append watch events, events is {}", events);
        Set<byte[]> keys = new HashSet<>();
        events.forEach(event -> keys.add(event.getKey()));

        if (this.shutdownLatch != null) {
            IllegalStateException e = new IllegalStateException("Service already shutdown.");
            keys.forEach(key -> {
                if (listeners.containsKey(key)) {
                    listeners.get(key).onError(e);
                }
            });
        }
        List<WatchEvent> validEvents = new ArrayList<>();
        for (WatchEvent event : events) {
            if(this.listeners.containsKey(event.getKey()))
                validEvents.add(event);
        }
        if(!validEvents.isEmpty()) {
            try {
                List<EventTranslator<WatchEvent>> translators = new ArrayList<>();
                for (WatchEvent validEvent : validEvents) {
                    EventTranslator<WatchEvent> translator = (newEvent, sequence) -> {
                        newEvent.setEventType(validEvent.getEventType());
                        newEvent.setKey(validEvent.getKey());
                        newEvent.setPreValue(validEvent.getPreValue());
                        newEvent.setValue(validEvent.getValue());
                    };
                    translators.add(translator);
                }
                int retryTimes = 0;
                while (true) {
                    if (this.watchRingBuffer.tryPublishEvents(translators.toArray(new EventTranslator[translators.size()]))) {
                        break;
                    } else {
                        retryTimes++;
                        if (retryTimes > MAX_ADD_REQUEST_RETRY_TIMES) {
//                        if(nodeMetrics != null)
//                            this.nodeMetrics.recordTimes("read-index-overload-times", 1);
//                        LOG.warn("Node {} WatchServiceImpl watchRingBuffer is overload.", node.getNodeId());
                            return;
                        }
                        ThreadHelper.onSpinWait();
                    }
                }
            } catch (final Exception e) {
                keys.forEach(key -> {
                    if (listeners.containsKey(key)) {
                        listeners.get(key).onError(e);
                    }
                });
            }
        }
    }

    @Override
    public boolean isWatched(byte[] key) {
        return listeners.containsKey(key);
    }

    @Override
    public void writeSnapshot(final String snapshotPath, String suffix) throws Exception {
        File file;
        if(StringUtils.isBlank(suffix))
            file = Paths.get(snapshotPath,"watch.snf").toFile();
        else{
            file = Paths.get(snapshotPath,"watch.snf."+suffix).toFile();
        }
        writeToFile(file);
    }

    @Override
    public void readSnapshot(final String snapshotPath, String suffix) throws Exception {
        File file;
        if(StringUtils.isBlank(suffix))
            file = Paths.get(snapshotPath,"watch.snf").toFile();
        else{
            file = Paths.get(snapshotPath,"watch.snf."+suffix).toFile();
        }
        readFromFile(file);
    }

    public void writeToFile(File file) throws Exception {
        try (final FileOutputStream out = new FileOutputStream(file);
             final BufferedOutputStream bufOutput = new BufferedOutputStream(out)) {
            final byte[] bytes = this.serializer.writeObject(this.listeners);
            final byte[] lenBytes = new byte[4];
            Bits.putInt(lenBytes, 0, bytes.length);
            bufOutput.write(lenBytes);
            bufOutput.write(bytes);
            bufOutput.flush();
            out.getFD().sync();
        }
    }

    public void readFromFile(File file) throws Exception {
        if (!file.exists()) {
            throw new NoSuchFieldException(file.getPath());
        }
        try (final FileInputStream in = new FileInputStream(file);
             final BufferedInputStream bufInput = new BufferedInputStream(in)) {
            final byte[] lenBytes = new byte[4];
            int read = bufInput.read(lenBytes);
            if (read != lenBytes.length) {
                throw new IOException("fail to read snapshot file length, expects " + lenBytes.length
                        + " bytes, but read " + read);
            }
            final int len = Bits.getInt(lenBytes, 0);
            final byte[] bytes = new byte[len];
            read = bufInput.read(bytes);
            if (read != bytes.length) {
                throw new IOException("fail to read snapshot file, expects " + bytes.length + " bytes, but read "
                        + read);
            }
            this.listeners = this.serializer.readObject(bytes, (new ConcurrentSkipListMap<byte[], WatchListener>()).getClass());
        }
    }
}
