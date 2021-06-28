package org.elasticsearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class TransportChannelResponseRunnable extends AbstractRunnable {
    private CheckedSupplier<TransportResponse, Exception> command;
    private TransportChannel channel;
    private String action;
    static private final Logger logger = LogManager.getLogger(TransportChannelResponseRunnable.class);

    public TransportChannelResponseRunnable(String action, CheckedSupplier<TransportResponse, Exception> command, TransportChannel channel) {
        this.action = action;
        this.command = command;
        this.channel = channel;
    }

    @Override
    public void onFailure(Exception e) {
        try {
            channel.sendResponse(e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "Failed to send error message back to client for action [{}]", action), inner);
        }
    }

    @Override
    protected void doRun() throws Exception {
        channel.sendResponse(command.get());
    }
}
