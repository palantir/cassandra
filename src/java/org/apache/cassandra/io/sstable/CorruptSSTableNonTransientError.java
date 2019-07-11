package org.apache.cassandra.io.sstable;

import org.apache.cassandra.service.StorageServiceMBean;

import java.nio.file.Path;
import java.util.Objects;

public class CorruptSSTableNonTransientError implements StorageServiceMBean.NonTransientError {

    public final Path path;

    public CorruptSSTableNonTransientError(Path path) {
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CorruptSSTableNonTransientError that = (CorruptSSTableNonTransientError) o;
        return Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }
}
