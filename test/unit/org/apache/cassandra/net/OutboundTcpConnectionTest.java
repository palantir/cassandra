/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.io.IVersionedSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OutboundTcpConnectionTest
{
    private OutboundTcpConnection connection;
    private static OutboundTcpConnectionPool pool;
    private static final InetAddress TARGET = mock(InetAddress.class);
    private static final OutboundTcpConnection.QueuedMessage QM1 = new OutboundTcpConnection.QueuedMessage(new MessageOut<>(MessagingService.Verb.MUTATION), 1);
    private static final OutboundTcpConnection.QueuedMessage QM2 = new OutboundTcpConnection.QueuedMessage(new MessageOut<>(MessagingService.Verb.MUTATION), 2);
    private static final OutboundTcpConnection.QueuedMessage QM3 = new OutboundTcpConnection.QueuedMessage(new MessageOut<>(MessagingService.Verb.MUTATION), 3);

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        pool = mock(OutboundTcpConnectionPool.class);
        doReturn(InetAddress.getLocalHost()).when(pool).endPoint();
    }

    @Before
    public void before() {
        connection = spy(new OutboundTcpConnection(pool, "test"));
        MessagingService.instance().clearCallbacksUnsafe();
    }

    @Test
    public void invokeFailureCallback_ignoresNonFailureCallbacks() {
        TestCallback cb = new TestCallback();
        CallbackInfo nonFailureCallback = new CallbackInfo(TARGET, cb, mock(IVersionedSerializer.class), false);
        MessagingService.instance().setCallbackForTests(QM1.id, nonFailureCallback);
        connection.invokeFailureCallback(QM1);
        assertEquals(0, cb.responses.get());
    }

    @Test
    public void invokeFailureCallback_handlesExpiredCallback() {
        assertNull(MessagingService.instance().getRegisteredCallback(QM1.id));
        connection.invokeFailureCallback(QM1);
    }

    @Test
    public void invokeFailureCallback_runsCallback() {
        TestFailureCallback cb = registerFailureCallback(QM1);
        connection.invokeFailureCallback(QM1);
        assertEquals(0, cb.responses.get());
        assertEquals(1, cb.failures.get());
        assertNull(MessagingService.instance().getRegisteredCallback(QM1.id));
    }

    @Test
    public void clearQueueWithFailureCallback_handlesInProgressDrainedList() throws InterruptedException
    {
        List<OutboundTcpConnection.QueuedMessage> drained = new ArrayList<>(2);
        drained.add(QM1);
        drained.add(QM2);
        BlockingQueue<OutboundTcpConnection.QueuedMessage> backlog = new LinkedBlockingQueue<>();
        backlog.put(QM3);

        TestFailureCallback cb1 = registerFailureCallback(QM1);
        TestFailureCallback cb2 = registerFailureCallback(QM2);
        TestFailureCallback cb3 = registerFailureCallback(QM3);

        connection.clearQueueWithFailureCallback(1, drained, 2, backlog);

        assertEquals(0, cb1.failures.get());
        assertEquals(1, cb2.failures.get());
        assertEquals(1, cb3.failures.get());

        assertTrue(drained.isEmpty());
        assertTrue(backlog.isEmpty());
    }

    @Test
    public void clearQueueWithFailureCallback_clearsLargeBacklog() throws InterruptedException
    {
        List<OutboundTcpConnection.QueuedMessage> drained = new ArrayList<>(2);
        BlockingQueue<OutboundTcpConnection.QueuedMessage> backlog = spy(new LinkedBlockingQueue<>());
        backlog.put(QM1);
        backlog.put(QM2);
        backlog.put(QM3);
        backlog.put(QM3);
        backlog.put(QM3);

        TestFailureCallback cb1 = registerFailureCallback(QM1);
        TestFailureCallback cb2 = registerFailureCallback(QM2);
        TestFailureCallback cb3 = registerFailureCallback(QM3);

        connection.clearQueueWithFailureCallback(0, drained, 2, backlog);
        // With enough elements remaining, drain the buffer size
        verify(backlog, times(2)).drainTo(anyCollection(), eq(2));
        // Last call, don't take more off the backlog than needed from when we first called clearQueueWithFailureCallback
        verify(backlog, times(1)).drainTo(anyCollection(), eq(1));

        assertEquals(1, cb1.failures.get());
        assertEquals(1, cb2.failures.get());
        assertEquals(1, cb3.failures.get());

        assertTrue(drained.isEmpty());
        assertTrue(backlog.isEmpty());
    }

    static class TestCallback<T> implements IAsyncCallback<T>
    {
        public final AtomicInteger responses = new AtomicInteger(0);

        public void response(MessageIn<T> _msg)
        {
            responses.incrementAndGet();

        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    static class TestFailureCallback<T> extends TestCallback<T> implements IAsyncCallbackWithFailure<T> {
        public final AtomicInteger failures = new AtomicInteger(0);

        public void onFailure(InetAddress from)
        {
            failures.incrementAndGet();
        }
    }

    private TestFailureCallback registerFailureCallback(OutboundTcpConnection.QueuedMessage qm) {
        TestFailureCallback cb = new TestFailureCallback();
        MessagingService.instance().setCallbackForTests(qm.id, new CallbackInfo(TARGET, cb, mock(IVersionedSerializer.class), true));
        return cb;
    }
}
