package com.palantir.cassandra.action.common;

import java.io.File;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.codehaus.jackson.map.ObjectMapper;

public final class CommitLogMetadata
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FILENAME = "cassandra-commitlog-metadata.json";

    private static final File METADATA_FILE = new File(DatabaseDescriptor.getCommitLogLocation(), FILENAME);

    @JsonProperty("host-id")
    public final UUID hostId;

    public CommitLogMetadata(UUID hostId)
    {
        this.hostId = hostId;
    }

    public static synchronized Optional<CommitLogMetadata> readFromFile() throws Exception
    {
        if (METADATA_FILE.exists())
        {
            return Optional.of(MAPPER.readValue(METADATA_FILE, CommitLogMetadata.class));
        }
        else
        {
            return Optional.empty();
        }
    }

    public static synchronized void writeToFile(UUID hostId) throws Exception
    {
        MAPPER.writeValue(METADATA_FILE, new CommitLogMetadata(hostId));
    }
}
