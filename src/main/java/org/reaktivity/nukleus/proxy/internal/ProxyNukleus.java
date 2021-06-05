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

import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.Nukleus;

public final class ProxyNukleus implements Nukleus
{
    public static final String NAME = "proxy";

    private final ProxyConfiguration config;

    ProxyNukleus(
        ProxyConfiguration config)
    {
        this.config = config;
    }

    @Override
    public String name()
    {
        return ProxyNukleus.NAME;
    }

    @Override
    public ProxyConfiguration config()
    {
        return config;
    }

    @Override
    public ProxyElektron supplyElektron(
        ElektronContext context)
    {
        return new ProxyElektron(config, context);
    }
}
