package com.mastfrog.giulius.settings.etcd;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Tim Boudreau
 */
class EtcdErrorHandlerImpl implements EtcdErrorHandler {

    @Override
    public void onException(Exception e) {
        Logger.getLogger(EtcdErrorHandlerImpl.class.getName()).log(Level.SEVERE, "Refresh exception", e);
    }
}
