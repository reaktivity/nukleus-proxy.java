/**
 * Copyright 2016-2020 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.proxy.internal.stream;

import static java.util.Objects.requireNonNull;

import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.proxy.internal.ProxyConfiguration;
import org.reaktivity.nukleus.proxy.internal.ProxyNukleus;
import org.reaktivity.nukleus.proxy.internal.types.OctetsFW;
import org.reaktivity.nukleus.proxy.internal.types.control.RouteFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.DataFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.EndFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ProxyClientFactory implements StreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final ProxyBeginExFW beginExRO = new ProxyBeginExFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final ProxyRouter router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;

    private final Long2ObjectHashMap<MessageConsumer> correlations;

    public ProxyClientFactory(
        ProxyConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        ToIntFunction<String> supplyTypeId)
    {
        this.router = new ProxyRouter(router, supplyTypeId.applyAsInt(ProxyNukleus.NAME));
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.correlations = new Long2ObjectHashMap<>();
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, sender);
        }
        else
        {
            newStream = newReplyStream(begin, sender);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final RouteFW route = router.resolve(begin);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long routeId = begin.routeId();
            final long initialId = begin.streamId();
            final long resolvedId = route.correlationId();

            newStream = new ProxyApplicationClient(routeId, initialId, sender, resolvedId)::onApplication;
        }

        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        return correlations.remove(replyId);
    }

    private final class ProxyApplicationClient
    {
        private final MessageConsumer receiver;
        private final long routeId;
        private final long initialId;
        private final long replyId;

        private final ProxyNetworkClient network;

        private int initialBudget;
        private int replyBudget;
        private int replyPadding;

        private ProxyApplicationClient(
            long routeId,
            long initialId,
            MessageConsumer receiver,
            long resolvedId)
        {
            this.routeId = routeId;
            this.initialId = initialId;
            this.receiver = receiver;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.network = new ProxyNetworkClient(this, resolvedId);
        }

        private void onApplication(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onApplicationBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onApplicationData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onApplicationEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onApplicationAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onApplicationWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onApplicationReset(reset);
                break;
            default:
                break;
            }
        }

        private void onApplicationBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long affinity = begin.affinity();
            final OctetsFW extension = begin.extension();

            final ProxyBeginExFW beginEx = extension.get(beginExRO::tryWrap);
            // TODO; encoder slot, encode beginEx, await window credit to flush

            network.doNetworkBegin(traceId, authorization, affinity);
        }

        private void onApplicationData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long authorization = data.authorization();
            final long budgetId = data.budgetId();
            final int flags = data.flags();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();

            initialBudget -= reserved;

            if (initialBudget < 0)
            {
                doApplicationReset(traceId, authorization);
                network.doNetworkAbort(traceId, authorization);
            }
            else
            {
                network.doNetworkData(traceId, authorization, budgetId, flags, reserved, payload);
            }
        }

        private void onApplicationEnd(
            EndFW end)
        {
            final long traceId = end.traceId();
            final long authorization = end.authorization();

            network.doNetworkEnd(traceId, authorization);
        }

        private void onApplicationAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            network.doNetworkAbort(traceId, authorization);
        }

        private void onApplicationWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyBudget += credit;
            replyPadding = padding;

            network.doNetworkWindow(traceId, authorization, budgetId, replyBudget, replyPadding);
        }

        private void onApplicationReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            network.doNetworkReset(traceId, authorization);
        }

        private void doApplicationBegin(
            long traceId,
            long authorization,
            long affinity)
        {
            router.setThrottle(replyId, this::onApplication);
            doBegin(receiver, routeId, replyId, traceId, authorization, affinity);
        }

        private void doApplicationData(
            long traceId,
            long authorization,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload)
        {
            replyBudget -= reserved;
            assert replyBudget >= 0;

            doData(receiver, routeId, replyId, traceId, authorization, flags, budgetId, reserved, payload);
        }

        private void doApplicationEnd(
            long traceId,
            long authorization)
        {
            doEnd(receiver, routeId, replyId, traceId, authorization);
        }

        private void doApplicationAbort(
            long traceId,
            long authorization)
        {
            doAbort(receiver, routeId, replyId, traceId, authorization);
        }

        private void doApplicationReset(
            long traceId,
            long authorization)
        {
            doReset(receiver, routeId, initialId, traceId, authorization);
        }

        private void doApplicationWindow(
            long traceId,
            long authorization,
            long budgetId,
            int maxBudget,
            int minPadding)
        {
            int initialCredit = maxBudget - initialBudget;
            if (initialCredit > 0)
            {
                initialBudget += initialCredit;
                int initialPadding = minPadding;

                doWindow(receiver, routeId, initialId, traceId, authorization, budgetId, initialCredit, initialPadding);
            }
        }

    }

    private final class ProxyNetworkClient
    {
        private final ProxyApplicationClient application;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;

        private ProxyNetworkClient(
            ProxyApplicationClient application,
            long routeId)
        {
            this.application = application;
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId =  supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);
        }

        private void onNetwork(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onNetworkBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onNetworkData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onNetworkEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onNetworkAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onNetworkWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onNetworkReset(reset);
                break;
            default:
                break;
            }
        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long affinity = begin.affinity();

            application.doApplicationBegin(traceId, authorization, affinity);
        }

        private void onNetworkData(
            DataFW data)
        {
            final long authorization = data.authorization();
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                doNetworkReset(traceId, authorization);
                application.doApplicationAbort(traceId, authorization);
            }
            else
            {
                application.doApplicationData(traceId, authorization, flags, budgetId, reserved, payload);
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long traceId = end.traceId();
            final long authorization = end.authorization();

            application.doApplicationEnd(traceId, authorization);
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            application.doApplicationAbort(traceId, authorization);
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            initialBudget += credit;
            initialPadding = padding;

            application.doApplicationWindow(traceId, authorization, budgetId, initialBudget, initialPadding);
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            application.doApplicationReset(traceId, authorization);
        }

        private void doNetworkBegin(
            long traceId,
            long authorization,
            long affinity)
        {
            correlations.put(replyId, this::onNetwork);
            router.setThrottle(replyId, this::onNetwork);
            doBegin(receiver, affinity, initialId, traceId, authorization, affinity);
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            int flags,
            int reserved,
            OctetsFW payload)
        {
            initialBudget -= reserved;
            assert initialBudget >= 0;

            doData(receiver, reserved, initialId, traceId, authorization, flags, budgetId, reserved, payload);
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            doEnd(receiver, authorization, initialId, traceId, authorization);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            doAbort(receiver, authorization, initialId, traceId, authorization);
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            correlations.remove(replyId);
            doReset(receiver, routeId, replyId, traceId, authorization);
        }

        private void doNetworkWindow(
            long traceId,
            long authorization,
            long budgetId,
            int maxBudget,
            int minPadding)
        {
            final int replyCredit = maxBudget - replyBudget;
            if (replyCredit > 0)
            {
                replyBudget += replyCredit;
                int replyPadding = minPadding;

                doWindow(receiver, routeId, replyId, traceId, authorization, budgetId, replyCredit, replyPadding);
            }
        }
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long affinity)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        int flags,
        long budgetId,
        int reserved,
        OctetsFW payload)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .flags(flags)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    void doWindow(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .build();

        receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }
}
