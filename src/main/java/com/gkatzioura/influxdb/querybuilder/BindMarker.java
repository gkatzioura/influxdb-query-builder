/*
 * Copyright 2018 Emmanouil Gkatziouras
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gkatzioura.influxdb.querybuilder;

public class BindMarker {
    static final BindMarker ANONYMOUS = new BindMarker(null);

    private final String name;

    BindMarker(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        if (name == null)
            return "?";

        return Utils.appendName(name, new StringBuilder(name.length() + 1).append(':')).toString();
    }
}
