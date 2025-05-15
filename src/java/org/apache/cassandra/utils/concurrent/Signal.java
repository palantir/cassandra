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

package org.apache.cassandra.utils.concurrent;

/**
 * A Signal is a one-time-use mechanism for a thread to wait for notification that some condition
 * state has transitioned that it may be interested in (and hence should check if it is).
 * It is potentially transient, i.e. the state can change in the meantime, it only indicates
 * that it should be checked, not necessarily anything about what the expected state should be.
 *
 * Signal implementations should never wake up spuriously, they are always woken up by a
 * signal() or signalAll().
 *
 * This abstract definition of Signal does not need to be tied to a WaitQueue.
 * Whilst RegisteredSignal is the main building block of Signals, this abstract
 * definition allows us to compose Signals in useful ways. The Signal is 'owned' by the
 * thread that registered itself with WaitQueue(s) to obtain the underlying RegisteredSignal(s);
 * only the owning thread should use a Signal.
 */
public interface Signal
{

    /**
     * @return true if signalled; once true, must be discarded by the owning thread.
     */
    public boolean isSignalled();

    /**
     * @return true if cancelled; once cancelled, must be discarded by the owning thread.
     */
    public boolean isCancelled();

    /**
     * @return isSignalled() || isCancelled(). Once true, the state is fixed and the Signal should be discarded
     * by the owning thread.
     */
    public boolean isSet();

    /**
     * atomically: cancels the Signal if !isSet(), or returns true if isSignalled()
     *
     * @return true if isSignalled()
     */
    public boolean checkAndClear();

    /**
     * Should only be called by the owning thread. Indicates the signal can be retired,
     * and if signalled propagates the signal to another waiting thread
     */
    public abstract void cancel();

    /**
     * Wait, without throwing InterruptedException, until signalled. On exit isSignalled() must be true.
     * If the thread is interrupted in the meantime, the interrupted flag will be set.
     */
    public void awaitUninterruptibly();

    /**
     * Wait until signalled, or throw an InterruptedException if interrupted before this happens.
     * On normal exit isSignalled() must be true; however if InterruptedException is thrown isCancelled()
     * will be true.
     * @throws InterruptedException
     */
    public void await() throws InterruptedException;

    /**
     * Wait until signalled, or the provided time is reached, or the thread is interrupted. If signalled,
     * isSignalled() will be true on exit, and the method will return true; if timedout, the method will return
     * false and isCancelled() will be true; if interrupted an InterruptedException will be thrown and isCancelled()
     * will be true.
     * @param nanos System.nanoTime() to wait until
     * @return true if signalled, false if timed out
     * @throws InterruptedException
     */
    public boolean awaitUntil(long nanos) throws InterruptedException;
}
