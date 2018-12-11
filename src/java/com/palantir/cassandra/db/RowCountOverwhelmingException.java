/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.cassandra.db;

// TODO: extract a base class for this and TombstoneOverwhelmingException if we add any more similar
// exceptions - currently we have multiple places where we catch both and handling logic is the same
public class RowCountOverwhelmingException extends RuntimeException
{
    private final int numRows;
    private final int numTombstonedRows;
    private final int numRequested;
    private final String ksName;
    private final String cfName;
    private final String lastRowName;
    private final String dataRangeInfo;

    public RowCountOverwhelmingException(int numRows,
                                         int numTombstonedRows,
                                         int numRequested,
                                         String ksName,
                                         String cfName,
                                         String lastRowName,
                                         String dataRangeInfo)
    {
        this.numRows = numRows;
        this.numTombstonedRows = numTombstonedRows;
        this.numRequested = numRequested;
        this.ksName = ksName;
        this.cfName = cfName;
        this.lastRowName = lastRowName;
        this.dataRangeInfo = dataRangeInfo;
    }

    public String getLocalizedMessage()
    {
        return getMessage();
    }

    public String getMessage()
    {
        return String.format(
                "Scanned over %d rows (%d tombstoned) in %s.%s; %d rows were requested; query aborted " +
                "(see rowcount_failure_threshold); lastRow=%s; dataLimits=%s",
                numRows, numTombstonedRows, ksName, cfName, numRequested, lastRowName, dataRangeInfo);
    }
}
