package play.modules.netty;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import play.Play;

import static org.jboss.netty.channel.Channels.pipeline;

public class HttpServerPipelineFactory implements ChannelPipelineFactory {

    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());

        Integer max = Integer.valueOf(Play.configuration.getProperty("module.netty.maxContentLength", "1048576"));
        if (max == -1) {
            max = Integer.MAX_VALUE;
        }
        pipeline.addLast("streamer", new ChunkedWriteHandler());
        pipeline.addLast("aggregator", new HttpChunkAggregator(max));
        //pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
        pipeline.addLast("handler", new PlayHandler());

        return pipeline;
    }
}

