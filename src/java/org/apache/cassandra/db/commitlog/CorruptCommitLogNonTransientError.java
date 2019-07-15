package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.service.StorageServiceMBean;

public final class CorruptCommitLogNonTransientError implements StorageServiceMBean.NonTransientError {

    public static CorruptCommitLogNonTransientError instance = new CorruptCommitLogNonTransientError();

    private CorruptCommitLogNonTransientError() {
    }
}
