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

import static org.reaktivity.reaktor.config.Role.CLIENT;
import static org.reaktivity.reaktor.config.Role.SERVER;

import java.util.EnumMap;
import java.util.Map;

import org.reaktivity.nukleus.proxy.internal.stream.ProxyClientFactory;
import org.reaktivity.nukleus.proxy.internal.stream.ProxyServerFactory;
import org.reaktivity.nukleus.proxy.internal.stream.ProxyStreamFactory;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Role;
import org.reaktivity.reaktor.nukleus.Elektron;
import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.stream.StreamFactory;

final class ProxyElektron implements Elektron
{
    private final Map<Role, ProxyStreamFactory> factories;

    ProxyElektron(
        ProxyConfiguration config,
        ElektronContext context)
    {
        final EnumMap<Role, ProxyStreamFactory> factories = new EnumMap<>(Role.class);
        factories.put(SERVER, new ProxyServerFactory(config, context));
        factories.put(CLIENT, new ProxyClientFactory(config, context));
        this.factories = factories;
    }

    @Override
    public StreamFactory attach(
        Binding binding)
    {
        ProxyStreamFactory factory = factories.get(binding.kind);
        if (factory != null)
        {
            factory.attach(binding);
        }
        return factory;
    }

    @Override
    public void detach(
        Binding binding)
    {
        ProxyStreamFactory factory = factories.get(binding.kind);
        if (factory != null)
        {
            factory.detach(binding.id);
        }
    }
}
