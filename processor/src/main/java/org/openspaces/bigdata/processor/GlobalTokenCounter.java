/*
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openspaces.bigdata.processor;

import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;
import static org.springframework.transaction.annotation.Propagation.REQUIRED;

import java.util.logging.Logger;

import javax.annotation.Resource;

import org.openspaces.bigdata.processor.events.TokenCounter;
import org.openspaces.core.GigaMap;
import org.openspaces.events.EventDriven;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.polling.Polling;
import org.openspaces.events.polling.ReceiveHandler;
import org.openspaces.events.polling.receive.MultiTakeReceiveOperationHandler;
import org.openspaces.events.polling.receive.ReceiveOperationHandler;
import org.springframework.transaction.annotation.Transactional;

/**
 * Polling event container polling for {@link TokenCounter} instances and updating atomic counters accordingly.
 * 
 * @author dotan
 */
@EventDriven
@Polling(gigaSpace = "gigaSpace", concurrentConsumers = 2, maxConcurrentConsumers = 2, receiveTimeout = 1000)
@TransactionalEvent
public class GlobalTokenCounter {
    private static final Logger log = Logger.getLogger(GlobalTokenCounter.class.getName());
    private static final int BATCH_SIZE = 100;

    @Resource
    GigaMap gigaMap;

    @ReceiveHandler
    ReceiveOperationHandler receiveHandler() {
        MultiTakeReceiveOperationHandler receiveHandler = new MultiTakeReceiveOperationHandler();
        receiveHandler.setMaxEntries(BATCH_SIZE);
        receiveHandler.setNonBlocking(true);
        receiveHandler.setNonBlockingFactor(1);
        return receiveHandler;
    }

    @EventTemplate
    TokenCounter tokenCounter() {
        return new TokenCounter();
    }

    @SpaceDataEvent
    public void eventListener(TokenCounter counter) {
        String token = counter.getToken();
        Integer count = counter.getCount();
        log.info("incrementing local token " + token + " by " + count);
        incrementLocalToken(token, count);
    }

    @SuppressWarnings("unchecked")
    @Transactional(readOnly = false, propagation = REQUIRED, isolation = READ_COMMITTED)
    private void incrementLocalToken(String token, Integer count) {
        Integer globalCount = gigaMap.containsKey(token) ? (Integer) gigaMap.get(token) + count : count;
        gigaMap.put(token, globalCount);
        log.fine("+++ token=" + token + " count=" + (Integer) gigaMap.get(token));
    }

}
