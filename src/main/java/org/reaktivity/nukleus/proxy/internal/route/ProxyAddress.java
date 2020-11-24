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
package org.reaktivity.nukleus.proxy.internal.route;

import static java.lang.Integer.parseInt;
import static org.agrona.Strings.parseIntOrDefault;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.bind.annotation.JsonbProperty;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressMatchFW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyAddressProtocol;
import org.reaktivity.specification.nukleus.proxy.internal.types.ProxyAddressFamily;

public class ProxyAddress
{
    private static final Pattern ADDRESS_RANGE_PATTERN = Pattern.compile("(?<prefix>[^/]+)(?:/(?<length>\\d+))?");
    private static final Pattern PORT_RANGE_PATTERN = Pattern.compile("(?<low>[\\d]+)(?:-(?<high>\\d+))?");

    @JsonbProperty
    public ProxyAddressFamily family;

    @JsonbProperty
    public ProxyAddressProtocol protocol;

    @JsonbProperty
    public String source;

    @JsonbProperty
    public String destination;

    @JsonbProperty
    public String sourcePort;

    @JsonbProperty
    public String destinationPort;

    void build(
        ProxyAddressMatchFW.Builder builder)
    {
        switch (family)
        {
        case INET:
            builder.inet(i -> i
                .protocol(p -> p.set(protocol))
                .source(s -> buildAddressRange(source,
                    (ip, l) -> s.prefix(p -> p.set(ip)).length(l)))
                .destination(d -> buildAddressRange(destination,
                    (ip, l) -> d.prefix(p -> p.set(ip)).length(l)))
                .sourcePort(s -> buildPortRange(sourcePort, (l, h) -> s.low(l).high(h)))
                .destinationPort(d -> buildPortRange(destinationPort, (l, h) -> d.low(l).high(h))));
            break;
        case INET6:
            builder.inet6(i -> i
                .protocol(p -> p.set(protocol))
                .source(s -> buildAddressRange(source,
                    (ip, l) -> s.prefix(p -> p.set(ip)).length(l)))
                .destination(d -> buildAddressRange(destination,
                    (ip, l) -> d.prefix(p -> p.set(ip)).length(l)))
                .sourcePort(s -> buildPortRange(sourcePort, (l, h) -> s.low(l).high(h)))
                .destinationPort(d -> buildPortRange(destinationPort, (l, h) -> d.low(l).high(h))));
            break;
        case UNIX:
            builder.unix(i -> i
                .protocol(p -> p.set(protocol))
                .source(s -> buildAddressRange(source,
                    (ip, l) -> s.prefix(p -> p.set(ip)).length(l)))
                .destination(d -> buildAddressRange(destination,
                    (ip, l) -> d.prefix(p -> p.set(ip)).length(l))));
            break;
        }
    }

    private static void buildAddressRange(
        String address,
        BiConsumer<byte[], Integer> setter)
    {
        final Matcher matcher = ADDRESS_RANGE_PATTERN.matcher(address);
        assert matcher.matches();

        final String prefix = matcher.group("prefix");
        final int length = parseIntOrDefault(matcher.group("length"), prefix.length());
        final InetAddress inet = getInetAddressByName(prefix);
        final byte[] ip = inet.getAddress();

        setter.accept(ip, length);
    }

    private static void buildPortRange(
        String port,
        BiConsumer<Integer, Integer> setter)
    {
        final Matcher matcher = PORT_RANGE_PATTERN.matcher(port);
        assert matcher.matches();

        final int low = parseInt(matcher.group("low"));
        final int high = parseIntOrDefault(matcher.group("high"), low);

        setter.accept(low, high);
    }

    private static InetAddress getInetAddressByName(
        String host)
    {
        InetAddress address = null;

        try
        {
            address = InetAddress.getByName(host);
        }
        catch (UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return address;
    }
}
