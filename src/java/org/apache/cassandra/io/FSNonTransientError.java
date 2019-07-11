package org.apache.cassandra.io;

import org.apache.cassandra.service.StorageServiceMBean;

public class FSNonTransientError implements StorageServiceMBean.NonTransientError {

    public static FSNonTransientError instance = new FSNonTransientError();

    private FSNonTransientError() {
    }
}
