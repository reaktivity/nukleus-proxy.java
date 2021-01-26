/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.proxy.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.util.concurrent.CompletableFuture;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.proxy.internal.route.ProxyExtension;
import org.reaktivity.nukleus.proxy.internal.types.Flyweight;
import org.reaktivity.nukleus.proxy.internal.types.OctetsFW;
import org.reaktivity.nukleus.proxy.internal.types.control.FreezeFW;
import org.reaktivity.nukleus.proxy.internal.types.control.ProxyRouteExFW;
import org.reaktivity.nukleus.proxy.internal.types.control.Role;
import org.reaktivity.nukleus.proxy.internal.types.control.RouteFW;
import org.reaktivity.nukleus.proxy.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.route.RouteKind;

public final class ProxyController implements Controller
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(0, 0), 0, 0);

    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();
    private final FreezeFW.Builder freezeRW = new FreezeFW.Builder();

    private final ProxyRouteExFW.Builder routeExRW = new ProxyRouteExFW.Builder();

    private final ControllerSpi controllerSpi;
    private final MutableDirectBuffer commandBuffer;
    private final MutableDirectBuffer extensionBuffer;

    public ProxyController(
        ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.commandBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.extensionBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<ProxyController> kind()
    {
        return ProxyController.class;
    }

    @Override
    public String name()
    {
        return ProxyNukleus.NAME;
    }

    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress)
    {
        return route(kind, localAddress, remoteAddress, null);
    }

    @Override
    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        String extension)
    {
        Flyweight routeEx = EMPTY_OCTETS;

        if (extension != null)
        {
            try (Jsonb jsonb = JsonbBuilder.create())
            {
                ProxyExtension proxyEx = jsonb.fromJson(extension, ProxyExtension.class);

                routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                        .address(proxyEx::buildAddress)
                        .infos(proxyEx::buildInfos)
                        .build();
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return doRoute(kind, localAddress, remoteAddress, routeEx);
    }

    public CompletableFuture<Void> unroute(
        long routeId)
    {
        final long correlationId = controllerSpi.nextCorrelationId();

        final UnrouteFW unroute = unrouteRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                     .correlationId(correlationId)
                                     .nukleus(name())
                                     .routeId(routeId)
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    public CompletableFuture<Void> freeze()
    {
        final long correlationId = controllerSpi.nextCorrelationId();

        final FreezeFW freeze = freezeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                  .correlationId(correlationId)
                                  .nukleus(name())
                                  .build();

        return controllerSpi.doFreeze(freeze.typeId(), freeze.buffer(), freeze.offset(), freeze.sizeof());
    }

    private CompletableFuture<Long> doRoute(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        Flyweight extension)
    {
        final long correlationId = controllerSpi.nextCorrelationId();
        final Role role = Role.valueOf(kind.ordinal());

        final RouteFW route = routeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                .correlationId(correlationId)
                .nukleus(name())
                .role(b -> b.set(role))
                .localAddress(localAddress)
                .remoteAddress(remoteAddress)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }
}
