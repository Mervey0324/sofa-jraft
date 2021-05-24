package com.alipay.sofa.jraft.rhea.watch;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AddListener {
    private byte[] key;
    private boolean prefix = false;
    private WatchListener listener;
}
