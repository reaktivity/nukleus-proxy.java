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
package org.reaktivity.nukleus.proxy.internal.config;

import static java.util.stream.Collectors.toList;

import java.util.List;

import org.reaktivity.nukleus.proxy.internal.types.stream.ProxyBeginExFW;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Role;

public final class ProxyBinding
{
    public final long routeId;
    public final String entry;
    public final Role kind;
    public final ProxyOptions options;
    public final List<ProxyRoute> routes;
    public final ProxyRoute exit;

    public ProxyBinding(
        Binding binding)
    {
        this.routeId = binding.id;
        this.entry = binding.entry;
        this.kind = binding.kind;
        this.options = ProxyOptions.class.cast(binding.options);
        this.routes = binding.routes.stream().map(ProxyRoute::new).collect(toList());
        this.exit = binding.exit != null ? new ProxyRoute(binding.exit) : null;
    }

    public ProxyRoute resolve(
        long authorization,
        ProxyBeginExFW beginEx)
    {
        return routes.stream()
                .filter(r -> r.when.stream().allMatch(c -> c.matches(beginEx)))
                .findFirst()
                .orElse(exit);
    }
}
