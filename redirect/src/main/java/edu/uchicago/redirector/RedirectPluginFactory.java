package edu.uchicago.redirector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.protocol.messages.ErrorResponse;

import com.google.common.base.Strings;
import org.jboss.netty.channel.ChannelHandler;

import static com.google.common.base.Preconditions.checkArgument;

public class RedirectPluginFactory implements ChannelHandlerFactory
{
    private static final String NAME = "redirector";

    private static final Set<String> ALTERNATIVE_NAMES = new HashSet<>(Arrays.asList(NAME));

    private final String host;
    private final int port;
    private final boolean useBackwardsCompatiblePlugin;

    public RedirectPluginFactory(Properties properties)
    {
        String host = properties.getProperty("xrootd.redirector.host");
        String port = properties.getProperty("xrootd.redirector.port");

        checkArgument(!Strings.isNullOrEmpty(host), "xrootd.redirector.host is a required property");
        checkArgument(!Strings.isNullOrEmpty(port), "xrootd.redirector.port is a required property");

        useBackwardsCompatiblePlugin = !hasErrorGetters();

        this.port = Integer.parseInt(port);
        this.host = host;
    }

    private boolean hasErrorGetters()
    {
        try {
            ErrorResponse.class.getMethod("getErrorNumber");
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    static boolean hasName(String name)
    {
        return ALTERNATIVE_NAMES.contains(name);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public String getDescription()
    {
        return "xrootd4j redirect plugin";
    }

    @Override
    public ChannelHandler createHandler()
    {
        return useBackwardsCompatiblePlugin ? new CompatibleRedirectPlugin(host, port) : new RedirectPlugin(host, port);
    }
}
