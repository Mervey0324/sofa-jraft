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

public class WatchServiceImpl implements WatchService {

    private static final int           MAX_ADD_REQUEST_RETRY_TIMES = 3;
    private static final Logger        LOG                         = LoggerFactory.getLogger(WatchServiceImpl.class);

    private Disruptor<WatchEvent>      watchDisruptor;
    private RingBuffer<WatchEvent>     watchRingBuffer;
    private WatchOptions               options;

    private static final Comparator<byte[]>              COMPARATOR   = BytesUtil.getDefaultByteArrayComparator();
    private ConcurrentNavigableMap<byte[], WatchListener> listeners;
    private ConcurrentNavigableMap<byte[], WatchListener> prefixListeners;
    private volatile CountDownLatch    shutdownLatch;

    private final Serializer           serializer                  = Serializers.getDefault();

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
            if (!prefixListeners.isEmpty()) {
                prefixListeners.keySet().forEach(key -> {
                    if(BytesUtil.isPrefix(event.getKey(), key))
                        prefixListeners.get(key).onNext(event);
                });
                LOG.info(">>>>>>>>> execute prefix listener onNext end.");
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
        prefixListeners = new ConcurrentSkipListMap<>(COMPARATOR);
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
    public void addListener(byte[] key, WatchListener listener, boolean prefix) {
        if(prefix)
            this.prefixListeners.put(key, listener);
        else
            this.listeners.put(key, listener);
    }

    @Override
    public void addListeners(List<AddListener> listeners) {
        listeners.forEach(listener -> {
            if (listener.isPrefix())
                this.prefixListeners.put(listener.getKey(), listener.getListener());
            else
                this.listeners.put(listener.getKey(), listener.getListener());
        });
    }

    @Override
    public void removeListener(byte[] key) {
        this.prefixListeners.remove(key);
        this.listeners.remove(key);
    }

    @Override
    public void removeListeners() {
        this.prefixListeners.clear();
        this.listeners.clear();
    }

    @Override
    public boolean isWatched(byte[] key) {
        if(listeners.containsKey(key))
            return true;
        for (byte[] k : prefixListeners.keySet()) {
            if(BytesUtil.isPrefix(key, k))
                return true;
        }
        return false;
    }

    @Override
    public Set<byte[]> getWatchedKeys(List<byte[]> keys) {
        Set<byte[]> watchedKeys = new HashSet<>();
        for (byte[] key : keys) {
            if(listeners.containsKey(key)) {
                watchedKeys.add(key);
                continue;
            }
            for (byte[] prefixKey : prefixListeners.keySet()) {
                if(BytesUtil.isPrefix(key, prefixKey)){
                    watchedKeys.add(key);
                    break;
                }
            }
        }
        return watchedKeys;
    }

    @Override
    public void appendEvent(WatchEvent event) {
        LOG.info("append watch event, event is {}", event);

        if (this.shutdownLatch != null) {
            IllegalStateException e = new IllegalStateException("Service already shutdown.");
            if (listeners.containsKey(event.getKey())) {
                listeners.get(event.getKey()).onError(e);
            }
            prefixListeners.keySet().forEach(key -> {
                if(BytesUtil.isPrefix(event.getKey(), key))
                    prefixListeners.get(key).onError(e);
            });
        }

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
    public void writeSnapshot(final String snapshotPath, String suffix) throws Exception {
        File file;
        if (StringUtils.isBlank(suffix))
            file = Paths.get(snapshotPath, "watch.snf").toFile();
        else {
            file = Paths.get(snapshotPath, "watch.snf." + suffix).toFile();
        }
        writeToFile(file);
    }

    @Override
    public void readSnapshot(final String snapshotPath, String suffix) throws Exception {
        File file;
        if (StringUtils.isBlank(suffix))
            file = Paths.get(snapshotPath, "watch.snf").toFile();
        else {
            file = Paths.get(snapshotPath, "watch.snf." + suffix).toFile();
        }
        readFromFile(file);
    }

    public void writeToFile(File file) throws Exception {
        try (final FileOutputStream out = new FileOutputStream(file);
             final BufferedOutputStream bufOutput = new BufferedOutputStream(out)) {
            // write listener
            final byte[] bytes = this.serializer.writeObject(this.listeners);
            final byte[] lenBytes = new byte[4];
            Bits.putInt(lenBytes, 0, bytes.length);
            bufOutput.write(lenBytes);
            bufOutput.write(bytes);

            // write prefix listener
            final byte[] bytes2 = this.serializer.writeObject(this.prefixListeners);
            final byte[] lenBytes2 = new byte[4];
            Bits.putInt(lenBytes2, 0, bytes2.length);
            bufOutput.write(lenBytes2);
            bufOutput.write(bytes2);

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

            // read listener
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

            // read prefix listener
            final byte[] lenBytes2 = new byte[4];
            int read2 = bufInput.read(lenBytes2);
            if (read2 != lenBytes2.length) {
                throw new IOException("fail to read snapshot file length, expects " + lenBytes2.length
                        + " bytes, but read " + read2);
            }
            final int len2 = Bits.getInt(lenBytes2, 0);
            final byte[] bytes2 = new byte[len2];
            read2 = bufInput.read(bytes2);
            if (read2 != bytes2.length) {
                throw new IOException("fail to read snapshot file, expects " + bytes2.length + " bytes, but read "
                        + read2);
            }
            this.prefixListeners = this.serializer.readObject(bytes2, (new ConcurrentSkipListMap<byte[], WatchListener>()).getClass());
        }
    }
}
