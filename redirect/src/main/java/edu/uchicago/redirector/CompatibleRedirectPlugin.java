package edu.uchicago.redirector;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureNotifier;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.RedirectResponse;

public class CompatibleRedirectPlugin extends SimpleChannelDownstreamHandler
{
	private final static Logger logger = LoggerFactory.getLogger(CompatibleRedirectPlugin.class);

    private final String host;
    private final int port;

    public CompatibleRedirectPlugin(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
        if (e.getMessage() instanceof ErrorResponse) {
            ErrorResponse error = (ErrorResponse) e.getMessage();
            logger.debug("redirector intercepted error: {}", error);
            if (error.getRequest() instanceof OpenRequest && error.toString().contains(String.valueOf(XrootdProtocol.kXR_NotFound)) ){
                logger.debug("redirecting upstream to {}:{}", host, port);
                ChannelFuture future = e.getChannel().write(new RedirectResponse(error.getRequest(), host, port, "", ""));
                future.addListener(new ChannelFutureNotifier(e.getFuture()));
                return;
            }
        }
        ctx.sendDownstream(e);
    }
}
