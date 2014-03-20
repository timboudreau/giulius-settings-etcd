package com.mastfrog.giulius.settings.etcd;

import com.google.inject.ImplementedBy;

/**
 *
 * @author Tim Boudreau
 */
@ImplementedBy(EtcdErrorHandlerImpl.class)
public interface EtcdErrorHandler {

    void onException(Exception e);
}
