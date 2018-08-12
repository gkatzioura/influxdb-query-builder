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

public class Ordering extends Utils.Appendeable {

    private final boolean isDesc;

    private static final String TIME_KEY = "time";

    /**
     * Influxdb ordering currently supports onlye time
     * @param isDesc
     */
    Ordering(boolean isDesc) {
        this.isDesc = isDesc;
    }

    @Override
    void appendTo(StringBuilder sb) {
        Utils.appendName(TIME_KEY, sb);
        sb.append(isDesc ? " DESC" : " ASC");
    }

}
