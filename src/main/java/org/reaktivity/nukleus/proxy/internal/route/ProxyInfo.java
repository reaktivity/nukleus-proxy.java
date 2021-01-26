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
package org.reaktivity.nukleus.proxy.internal.route;

import javax.json.bind.annotation.JsonbProperty;

import org.reaktivity.nukleus.proxy.internal.types.Array32FW;
import org.reaktivity.nukleus.proxy.internal.types.ProxyInfoFW;

public class ProxyInfo
{
    @JsonbProperty(nillable = true)
    public String alpn;

    @JsonbProperty(nillable = true)
    public String authority;

    @JsonbProperty(nillable = true)
    public byte[] identity;

    @JsonbProperty(nillable = true)
    public String namespace;

    @JsonbProperty(nillable = true)
    public ProxySecureInfo secure;

    void build(
        Array32FW.Builder<ProxyInfoFW.Builder, ProxyInfoFW> infos)
    {
        if (alpn != null)
        {
            infos.item(i -> i.alpn(alpn));
        }

        if (authority != null)
        {
            infos.item(i -> i.authority(authority));
        }

        if (identity != null)
        {
            infos.item(i -> i.identity(v -> v.value(m -> m.set(identity))));
        }

        if (namespace != null)
        {
            infos.item(i -> i.namespace(namespace));
        }

        if (secure != null)
        {
            secure.build(infos);
        }
    }
}
