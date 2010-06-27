/**
 *
 * Copyright 2010, Nicolas Leroux.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 *
 * User: nicolas
 * Date: Feb 25, 2010
 *
 */
package play.modules.netty;

import org.apache.commons.io.IOUtils;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;
import play.Play;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.UUID;

public class StreamChunkAggregator extends SimpleChannelUpstreamHandler {

    private volatile HttpMessage currentMessage;
    private volatile OutputStream out;
    private final int maxContentLength;
    private volatile File file;

    /**
     * Creates a new instance.
     */
    public StreamChunkAggregator(int maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {

        Object msg = e.getMessage();
        if (!(msg instanceof HttpMessage) && !(msg instanceof HttpChunk)) {
            ctx.sendUpstream(e);
            return;
        }

        HttpMessage currentMessage = this.currentMessage;
        File localFile = this.file;
        if (currentMessage == null) {

            HttpMessage m = (HttpMessage) msg;
            if (m.isChunked()) {
                final String localName = UUID.randomUUID().toString();

                // A chunked message - remove 'Transfer-Encoding' header,
                // initialize the cumulative buffer, and wait for incoming chunks.
                List<String> encodings = m.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING);
                encodings.remove(HttpHeaders.Values.CHUNKED);
                if (encodings.isEmpty()) {
                    m.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
                }

                this.currentMessage = m;
                this.file = new File(Play.tmpDir, localName);
                this.out = new FileOutputStream(file, true);
            } else {
                // Not a chunked message - pass through.
                ctx.sendUpstream(e);
            }
        } else {
            // TODO: If less that threshold then in memory
            // Merge the received chunk into the content of the current message.
            final HttpChunk chunk = (HttpChunk) msg;
            if (maxContentLength != -1 && (localFile.length() > (maxContentLength - chunk.getContent().readableBytes()))) {
                currentMessage.setHeader(
                        HttpHeaders.Names.WARNING, "play.netty.content.length.exceeded");
            } else {
                IOUtils.copyLarge(new ChannelBufferInputStream(chunk.getContent()), this.out);

                if (chunk.isLast()) {
                    this.out.flush();
                    this.out.close();

                    currentMessage.setHeader(
                            HttpHeaders.Names.CONTENT_LENGTH,
                            String.valueOf(localFile.length()));

                    currentMessage.setContent(new FileChannelBuffer(localFile));
                    this.out = null;
                    this.currentMessage = null;
                    this.file = null;
                    Channels.fireMessageReceived(ctx, currentMessage, e.getRemoteAddress());
                }
            }
        }
    }

}



