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

import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.proxy.internal.types.Array32FW;
import org.reaktivity.nukleus.proxy.internal.types.OctetsFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressInet4FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressInet6FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressInetFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressMatchFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressMatchInet4FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressMatchInet6FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressMatchInetFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressMatchUnixFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressProtocolFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressRangeInet4FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressRangeInet6FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressRangeInetFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressRangeUnixFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressUnixFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyInfoFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyPortRangeFW;
import org.reaktivity.nukleus.proxy.internal.types.String16FW;
import org.reaktivity.nukleus.proxy.internal.types.control.ProxyRouteExFW;
import org.reaktivity.nukleus.proxy.internal.types.control.RouteFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.proxy.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.nukleus.route.RouteManager;

public final class ProxyRouter
{
    private final RouteFW routeRO = new RouteFW();
    private final ProxyBeginExFW beginExRO = new ProxyBeginExFW();
    private final ProxyRouteExFW routeExRO = new ProxyRouteExFW();

    private final DirectBuffer prefixBuf = new UnsafeBuffer();
    private final DirectBuffer addressBuf = new UnsafeBuffer();
    private final MutableBoolean matchInfos = new MutableBoolean();

    private final RouteManager router;
    private final int typeId;

    private ProxyAddressFW address;
    private Array32FW<ProxyInfoFW> infos;

    public ProxyRouter(
        RouteManager router,
        int typeId)
    {
        this.router = router;
        this.typeId = typeId;
    }

    public int typeId()
    {
        return typeId;
    }

    public RouteFW resolveApp(
        BeginFW begin)
    {
        final long routeId = begin.routeId();
        final long authorization = begin.authorization();
        final OctetsFW extension = begin.extension();
        final ProxyBeginExFW beginEx = extension.get(beginExRO::tryWrap);

        return resolve(routeId, authorization, beginEx);
    }

    public RouteFW resolveNet(
        long routeId,
        long authorization,
        ProxyBeginExFW beginEx)
    {
        return resolve(routeId, authorization, beginEx);
    }

    public MessageConsumer supplyReceiver(
        long streamId)
    {
        return router.supplyReceiver(streamId);
    }

    public void setThrottle(
        long streamId,
        MessageConsumer throttle)
    {
        router.setThrottle(streamId, throttle);
    }

    private RouteFW resolve(
        long routeId,
        long authorization,
        ProxyBeginExFW beginEx)
    {
        this.address = beginEx != null && beginEx.typeId() == typeId ? beginEx.address() : null;
        this.infos = beginEx != null && beginEx.typeId() == typeId ? beginEx.infos() : null;
        return router.resolve(routeId, authorization, this::filter, this::map);
    }

    private RouteFW map(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private boolean filter(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final RouteFW route = routeRO.wrap(buffer, index, index + length);
        final ProxyRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);

        if (routeEx == null)
        {
            return true;
        }

        if (address == null)
        {
            return false;
        }

        return matchesAddress(routeEx.address(), address) &&
                matchesInfos(routeEx.infos(), infos);
    }

    private boolean matchesAddress(
        ProxyAddressMatchFW addressMatch,
        ProxyAddressFW address)
    {
        if (addressMatch.kind() == address.kind())
        {
            switch (addressMatch.kind())
            {
            case INET:
                return matchesAddressInet(addressMatch.inet(), address.inet());
            case INET4:
                return matchesAddressInet4(addressMatch.inet4(), address.inet4());
            case INET6:
                return matchesAddressInet6(addressMatch.inet6(), address.inet6());
            case UNIX:
                return matchesAddressUnix(addressMatch.unix(), address.unix());
            case NONE:
                break;
            }
        }

        return false;
    }

    private boolean matchesProtocol(
        ProxyAddressProtocolFW protocolMatch,
        ProxyAddressProtocolFW protocol)
    {
        return protocolMatch.get() == protocol.get();
    }

    boolean matchesAddressRange(
        OctetsFW address,
        OctetsFW prefix,
        int length)
    {
        prefixBuf.wrap(prefix.value(), 0, length);
        addressBuf.wrap(address.value(), 0, length);

        return prefixBuf.equals(addressBuf);
    }

    private boolean matchesPortRange(
        ProxyPortRangeFW portRange,
        int port)
    {
        final int low = portRange.low();
        final int high = portRange.high();

        return low <= port && port <= high;
    }

    private boolean matchesAddressRangeInet(
        ProxyAddressRangeInetFW addressRange,
        String16FW address)
    {
        final String pattern = addressRange.pattern().asString();
        final String regex = pattern.replaceAll("\\*", ".*").replaceAll("\\.", "\\.");
        return Pattern.compile(regex).matcher(address.asString()).matches();
    }

    private boolean matchesAddressRangeInet4(
        ProxyAddressRangeInet4FW addressRange,
        OctetsFW address)
    {
        final OctetsFW prefix = addressRange.prefix();
        final int length = addressRange.length();

        return matchesAddressRange(address, prefix, length);
    }

    private boolean matchesAddressRangeInet6(
        ProxyAddressRangeInet6FW addressRange,
        OctetsFW address)
    {
        final OctetsFW prefix = addressRange.prefix();
        final int length = addressRange.length();

        return matchesAddressRange(address, prefix, length);
    }

    private boolean matchesAddressRangeUnix(
        ProxyAddressRangeUnixFW addressRange,
        OctetsFW address)
    {
        final OctetsFW prefix = addressRange.prefix();
        final int length = addressRange.length();

        return matchesAddressRange(address, prefix, length);
    }

    private boolean matchesAddressInet(
        ProxyAddressMatchInetFW inetMatch,
        ProxyAddressInetFW inet)
    {
        return matchesProtocol(inetMatch.protocol(), inet.protocol()) &&
                matchesAddressRangeInet(inetMatch.source(), inet.source()) &&
                matchesAddressRangeInet(inetMatch.destination(), inet.destination()) &&
                matchesPortRange(inetMatch.sourcePort(), inet.sourcePort()) &&
                matchesPortRange(inetMatch.destinationPort(), inet.destinationPort());
    }

    private boolean matchesAddressInet4(
        ProxyAddressMatchInet4FW inet4Match,
        ProxyAddressInet4FW inet4)
    {
        return matchesProtocol(inet4Match.protocol(), inet4.protocol()) &&
                matchesAddressRangeInet4(inet4Match.source(), inet4.source()) &&
                matchesAddressRangeInet4(inet4Match.destination(), inet4.destination()) &&
                matchesPortRange(inet4Match.sourcePort(), inet4.sourcePort()) &&
                matchesPortRange(inet4Match.destinationPort(), inet4.destinationPort());
    }

    private boolean matchesAddressInet6(
        ProxyAddressMatchInet6FW inet6Match,
        ProxyAddressInet6FW inet6)
    {
        return matchesProtocol(inet6Match.protocol(), inet6.protocol()) &&
                matchesAddressRangeInet6(inet6Match.source(), inet6.source()) &&
                matchesAddressRangeInet6(inet6Match.destination(), inet6.destination()) &&
                matchesPortRange(inet6Match.sourcePort(), inet6.sourcePort()) &&
                matchesPortRange(inet6Match.destinationPort(), inet6.destinationPort());
    }

    private boolean matchesAddressUnix(
        ProxyAddressMatchUnixFW unixMatch,
        ProxyAddressUnixFW unix)
    {
        return matchesProtocol(unixMatch.protocol(), unix.protocol()) &&
                matchesAddressRangeUnix(unixMatch.source(), unix.source()) &&
                matchesAddressRangeUnix(unixMatch.destination(), unix.destination());
    }

    private boolean matchesInfos(
        Array32FW<ProxyInfoFW> infosMatch,
        Array32FW<ProxyInfoFW> infos)
    {
        matchInfos.value = true;
        infosMatch.forEach(i -> matchInfos.value &= infos.anyMatch(i::equals));
        return matchInfos.value;
    }
}
