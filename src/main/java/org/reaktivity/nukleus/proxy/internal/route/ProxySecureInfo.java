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

import javax.json.bind.annotation.JsonbProperty;

import org.reaktivity.nukleus.proxy.internal.types.Array32FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyInfoFW;

public class ProxySecureInfo
{
    @JsonbProperty(nillable = true)
    public String protocol;

    @JsonbProperty(nillable = true)
    public String cipher;

    @JsonbProperty(nillable = true)
    public String key;

    @JsonbProperty(nillable = true)
    public String name;

    @JsonbProperty(nillable = true)
    public String signature;

    void build(
        Array32FW.Builder<ProxyInfoFW.Builder, ProxyInfoFW> infos)
    {
        if (cipher != null)
        {
            infos.item(i -> i.secure(s -> s.cipher(cipher)));
        }

        if (key != null)
        {
            infos.item(i -> i.secure(s -> s.key(key)));
        }

        if (name != null)
        {
            infos.item(i -> i.secure(s -> s.name(name)));
        }

        if (protocol != null)
        {
            infos.item(i -> i.secure(s -> s.protocol(protocol)));
        }

        if (signature != null)
        {
            infos.item(i -> i.secure(s -> s.signature(signature)));
        }
    }
}
