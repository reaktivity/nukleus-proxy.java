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
package org.reaktivity.nukleus.proxy.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.proxy.internal.types.ProxyAddressProtocol.STREAM;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;
import static org.reaktivity.specification.nukleus.proxy.internal.types.ProxyAddressFamily.INET4;
import static org.reaktivity.specification.nukleus.proxy.internal.types.ProxyAddressFamily.INET6;
import static org.reaktivity.specification.nukleus.proxy.internal.types.ProxyAddressFamily.UNIX;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.proxy.internal.ProxyController;
import org.reaktivity.nukleus.proxy.internal.route.ProxyAddress;
import org.reaktivity.nukleus.proxy.internal.route.ProxyExtension;
import org.reaktivity.nukleus.proxy.internal.route.ProxyInfo;
import org.reaktivity.nukleus.proxy.internal.route.ProxySecureInfo;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/proxy/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/proxy/control/unroute")
        .addScriptRoot("freeze", "org/reaktivity/specification/nukleus/control/freeze");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("proxy"::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/server/nukleus"
    })
    public void shouldRouteServer() throws Exception
    {
        k3po.start();

        reaktor.controller(ProxyController.class)
               .route(SERVER, "net#0", "app#0")
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.tcp4/nukleus"
    })
    public void shouldRouteServerTcp4() throws Exception
    {
        k3po.start();

        try (Jsonb jsonb = JsonbBuilder.create())
        {
            final ProxyExtension extension = new ProxyExtension()
            {
                {
                    address = new ProxyAddress()
                    {
                        {
                            family = INET4;
                            protocol = STREAM;
                            source = "0.0.0.0/0";
                            destination = "0.0.0.0/0";
                            sourcePort = "0-65535";
                            destinationPort = "443";
                        }
                    };
                }
            };

            reaktor.controller(ProxyController.class)
                .route(SERVER, "net#0", "app#0", jsonb.toJson(extension))
                .get();
        }

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.tcp4.alpn/nukleus"
    })
    public void shouldRouteServerTcp4Alpn() throws Exception
    {
        k3po.start();

        try (Jsonb jsonb = JsonbBuilder.create())
        {
            final ProxyExtension extension = new ProxyExtension()
            {
                {
                    address = new ProxyAddress()
                    {
                        {
                            family = INET4;
                            protocol = STREAM;
                            source = "0.0.0.0/0";
                            destination = "0.0.0.0/0";
                            sourcePort = "0-65535";
                            destinationPort = "443";
                        }
                    };
                    info = new ProxyInfo()
                    {
                        {
                            alpn = "echo";
                        }
                    };
                }
            };

            reaktor.controller(ProxyController.class)
                .route(SERVER, "net#0", "app#0", jsonb.toJson(extension))
                .get();
        }

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.tcp4.ssl.version/nukleus"
    })
    public void shouldRouteServerTcp4SslVersion() throws Exception
    {
        k3po.start();

        try (Jsonb jsonb = JsonbBuilder.create())
        {
            final ProxyExtension extension = new ProxyExtension()
            {
                {
                    address = new ProxyAddress()
                    {
                        {
                            family = INET4;
                            protocol = STREAM;
                            source = "0.0.0.0/0";
                            destination = "0.0.0.0/0";
                            sourcePort = "0-65535";
                            destinationPort = "443";
                        }
                    };
                    info = new ProxyInfo()
                    {
                        {
                            secure = new ProxySecureInfo()
                            {
                                {
                                    protocol = "TLSv1.3";
                                }
                            };
                        }
                    };
                }
            };

            reaktor.controller(ProxyController.class)
                .route(SERVER, "net#0", "app#0", jsonb.toJson(extension))
                .get();
        }

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.tcp6/nukleus"
    })
    public void shouldRouteServerTcp6() throws Exception
    {
        k3po.start();

        try (Jsonb jsonb = JsonbBuilder.create())
        {
            final ProxyExtension extension = new ProxyExtension()
            {
                {
                    address = new ProxyAddress()
                    {
                        {
                            family = INET6;
                            protocol = STREAM;
                            source = "::0/0";
                            destination = "::0/0";
                            sourcePort = "0-65535";
                            destinationPort = "443";
                        }
                    };
                }
            };

            reaktor.controller(ProxyController.class)
                .route(SERVER, "net#0", "app#0", jsonb.toJson(extension))
                .get();
        }

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.sock.stream/nukleus"
    })
    public void shouldRouteServerSockStream() throws Exception
    {
        k3po.start();

        try (Jsonb jsonb = JsonbBuilder.create())
        {
            final ProxyExtension extension = new ProxyExtension()
            {
                {
                    address = new ProxyAddress()
                    {
                        {
                            family = UNIX;
                            protocol = STREAM;
                            source = "";
                            destination = "";
                        }
                    };
                }
            };

            reaktor.controller(ProxyController.class)
                .route(SERVER, "net#0", "app#0", jsonb.toJson(extension))
                .get();
        }

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/nukleus"
    })
    public void shouldRouteClient() throws Exception
    {
        k3po.start();

        reaktor.controller(ProxyController.class)
               .route(CLIENT, "app#0", "net#0")
               .get();

        k3po.finish();
    }


    @Test
    @Specification({
        "${route}/server/nukleus",
        "${unroute}/server/nukleus"
    })
    public void shouldUnrouteServer() throws Exception
    {
        k3po.start();

        long routeId = reaktor.controller(ProxyController.class)
                  .route(SERVER, "net#0", "app#0")
                  .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(ProxyController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/nukleus",
        "${unroute}/client/nukleus"
    })
    public void shouldUnrouteClient() throws Exception
    {
        k3po.start();

        long routeId = reaktor.controller(ProxyController.class)
                  .route(CLIENT, "app#0", "net#0")
                  .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(ProxyController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${freeze}/nukleus"
    })
    @ScriptProperty("nameF00N \"proxy\"")
    public void shouldFreeze() throws Exception
    {
        k3po.start();

        reaktor.controller(ProxyController.class)
               .freeze()
               .get();

        k3po.finish();
    }
}
