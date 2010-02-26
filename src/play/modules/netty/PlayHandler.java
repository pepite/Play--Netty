package play.modules.netty;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.stream.ChunkedNioFile;
import org.jboss.netty.handler.stream.ChunkedStream;
import play.Invoker;
import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.data.validation.Validation;
import play.exceptions.PlayException;
import play.libs.MimeTypes;
import play.mvc.ActionInvoker;
import play.mvc.Http;
import play.mvc.Http.Request;
import play.mvc.Http.Response;
import play.mvc.Router;
import play.mvc.Scope;
import play.mvc.results.NotFound;
import play.mvc.results.RenderStatic;
import play.templates.TemplateLoader;
import play.utils.Utils;
import play.vfs.VirtualFile;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.*;


@ChannelPipelineCoverage("one")
public class PlayHandler extends SimpleChannelUpstreamHandler {

    private final static String signature = "Play! Framework;" + Play.version + ";" + Play.mode.name().toLowerCase();

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            final HttpRequest nettyRequest = (HttpRequest) msg;
            final Request request = parseRequest(ctx, nettyRequest);
            final Response response = new Response();

            try {
                Http.Response.current.set(response);
                response.out = new ByteArrayOutputStream();
                boolean raw = false;
                for (PlayPlugin plugin : Play.plugins) {
                    if (plugin.rawInvocation(request, response)) {
                        raw = true;
                        break;
                    }
                }
                if (raw) {
                    copyResponse(ctx, request, response, nettyRequest);
                } else {
                    Invoker.invoke(new NettyInvocation(request, response, ctx, nettyRequest));
                }
            } catch (Exception ex) {
                serve500(ex, ctx);
                return;
            }
        }
    }

    private static Map<String, RenderStatic> staticPathsCache = new HashMap();

    public class NettyInvocation extends Invoker.DirectInvocation {


        private final ChannelHandlerContext ctx;
        private final Request request;
        private final Response response;
        private final HttpRequest nettyRequest;

        public NettyInvocation(Request request, Response response, ChannelHandlerContext ctx, HttpRequest nettyRequest) {
            this.ctx = ctx;
            this.request = request;
            this.response = response;
            this.nettyRequest = nettyRequest;
        }

        @Override
        public boolean init() {
            Request.current.set(request);
            Response.current.set(response);
            // Patch favicon.ico
            if (!request.path.equals("/favicon.ico")) {
                super.init();
            }
            if (Play.mode == Play.Mode.PROD && staticPathsCache.containsKey(request.path)) {
                RenderStatic rs = null;
                synchronized (staticPathsCache) {
                    rs = staticPathsCache.get(request.path);
                }
                serveStatic(rs, ctx, request, response, nettyRequest);
                return false;
            }
            try {
                Router.routeOnlyStatic(request);
            } catch (NotFound e) {
                serve404(e, ctx, request);
                return false;
            } catch (RenderStatic e) {
                if (Play.mode == Play.Mode.PROD) {
                    synchronized (staticPathsCache) {
                        staticPathsCache.put(request.path, e);
                    }
                }
                serveStatic(e, ctx, request, response, nettyRequest);
                return false;
            }
            return true;
        }

        @Override
        public void run() {
            try {
                super.run();
            } catch (Exception e) {
                e.printStackTrace();
                serve500(e, ctx);
            }
        }

        @Override
        public void execute() throws Exception {
            ActionInvoker.invoke(request, response);
            copyResponse(ctx, request, response, nettyRequest);
        }
    }


    protected static void addToResponse(Response response, HttpResponse nettyResponse) {
        Map<String, Http.Header> headers = response.headers;
        for (Map.Entry<String, Http.Header> entry : headers.entrySet()) {
            Http.Header hd = entry.getValue();
            for (String value : hd.values) {
                nettyResponse.setHeader(entry.getKey(), value);
            }
        }
        CookieEncoder encoder = new CookieEncoder(true);
        Map<String, Http.Cookie> cookies = response.cookies;
        for (Http.Cookie cookie : cookies.values()) {

            Cookie c = new DefaultCookie(cookie.name, cookie.value);
            c.setSecure(cookie.secure);
            c.setPath(cookie.path);
            if (cookie.domain != null) {
                c.setDomain(cookie.domain);
            }
            if (cookie.maxAge != null) {
                c.setMaxAge(cookie.maxAge);
            }
            encoder.addCookie(c);
            nettyResponse.addHeader("Set-Cookie", encoder.encode());
        }

        if (!response.headers.containsKey("cache-control") && !response.headers.containsKey("Cache-Control")) {
            nettyResponse.setHeader("Cache-Control", "no-cache");
        }

    }

    protected static void writeResponse(ChannelHandlerContext ctx, Response response, HttpResponse nettyResponse, HttpRequest nettyRequest) throws IOException {
        ChannelBuffer buf = ChannelBuffers.copiedBuffer(response.out.toByteArray());
        response.out.close();
        nettyResponse.setContent(buf);

        ChannelFuture f = ctx.getChannel().write(nettyResponse);
        f.addListener(ChannelFutureListener.CLOSE);

    }

    public static void copyResponse(ChannelHandlerContext ctx, Request request, Response response, HttpRequest nettyRequest) throws Exception {
        response.out.flush();
        HttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(response.status));
        nettyResponse.setHeader("Server", signature);


        if (response.contentType != null) {
            nettyResponse.setHeader("Content-Type", response.contentType + (response.contentType.startsWith("text/") && !response.contentType.contains("charset") ? "; charset=utf-8" : ""));
        } else {
            nettyResponse.setHeader("Content-Type", "text/plain; charset=utf-8");
        }

        addToResponse(response, nettyResponse);

        final Object obj = response.direct;
        File file = null;
        InputStream is = null;
        if (obj instanceof File) {
            file = (File) obj;
        } else if (obj instanceof InputStream) {
            is = (InputStream) obj;
        }

        if ((file != null) && file.isFile()) {

            try {
                addEtag(request, nettyResponse, file);
                nettyResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(file.length()));
                ctx.getChannel().write(nettyResponse);
                ChannelFuture writeFuture = ctx.getChannel().write(new ChunkedNioFile(file));
                // Decide whether to close the connection or not.
                boolean close =
                        HttpHeaders.Values.CLOSE.equalsIgnoreCase(nettyRequest.getHeader(HttpHeaders.Names.CONNECTION)) ||
                                nettyRequest.getProtocolVersion().equals(HttpVersion.HTTP_1_0) &&
                                        !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(nettyRequest.getHeader(HttpHeaders.Names.CONNECTION));

                if (close) {
                    // Close the connection when the whole content is written out.
                    writeFuture.addListener(ChannelFutureListener.CLOSE);
                }


            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        } else if (is != null) {
            ctx.getChannel().write(nettyResponse);
            ChannelFuture writeFuture = ctx.getChannel().write(new ChunkedStream(is));
            // Decide whether to close the connection or not.
            boolean close =
                    HttpHeaders.Values.CLOSE.equalsIgnoreCase(nettyRequest.getHeader(HttpHeaders.Names.CONNECTION)) ||
                            nettyRequest.getProtocolVersion().equals(HttpVersion.HTTP_1_0) &&
                                    !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(nettyRequest.getHeader(HttpHeaders.Names.CONNECTION));

            if (close) {
                // Close the connection when the whole content is written out.
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }


        } else {
            writeResponse(ctx, response, nettyResponse, nettyRequest);
        }

    }

    private static String escapeIllegalCharacters(String uri) throws Exception {
        // Encode uri according to RFC 2396
        return URLEncoder.encode(uri, "US-ASCII").replaceAll("%2F", "/").replaceAll("%3F", "?").replaceAll("%3D", "=").replaceAll("%26", "&");
    }

    public static Request parseRequest(ChannelHandlerContext ctx, HttpRequest nettyRequest) throws Exception {

        final URI uri = new URI(escapeIllegalCharacters(nettyRequest.getUri()));

        final Request request = new Request();
        request.remoteAddress = ctx.getChannel().getRemoteAddress().toString();
        request.method = nettyRequest.getMethod().getName();
        request.path = uri.getPath();
        request.querystring = uri.getQuery() == null ? "" : uri.getQuery();

        final String contentType = nettyRequest.getHeader("Content-Type");
        if (contentType != null) {
            request.contentType = contentType.split(";")[0].trim().toLowerCase();
        } else {
            request.contentType = "text/html";
        }

        ChannelBuffer b = nettyRequest.getContent();
        if (b instanceof FileChannelBuffer) {
            FileChannelBuffer buffer = (FileChannelBuffer) nettyRequest.getContent();
            request.body = buffer.getInputStream();
        } else {
            request.body = new ChannelBufferInputStream(b);
        }

        request.url = uri.toASCIIString();
        request.host = nettyRequest.getHeader("host");

        if (request.host.contains(":")) {
            final String[] host = request.host.split(":");
            request.port = Integer.parseInt(host[1]);
            request.domain = host[0];
        } else {
            request.port = 80;
            request.domain = request.host;
        }

        if (Play.configuration.containsKey("XForwardedSupport") && nettyRequest.getHeader("X-Forwarded-For") != null) {
            if (!Arrays.asList(Play.configuration.getProperty("XForwardedSupport", "127.0.0.1").split(",")).contains(request.remoteAddress)) {
                throw new RuntimeException("This proxy request is not authorized");
            } else {
                request.secure = ("https".equals(Play.configuration.get("XForwardedProto")) || "https".equals(nettyRequest.getHeader("X-Forwarded-Proto")) || "on".equals(nettyRequest.getHeader("X-Forwarded-Ssl")));
                if (Play.configuration.containsKey("XForwardedHost")) {
                    request.host = (String) Play.configuration.get("XForwardedHost");
                } else if (nettyRequest.getHeader("X-Forwarded-Host") != null) {
                    request.host = nettyRequest.getHeader("X-Forwarded-Host");
                }
                if (nettyRequest.getHeader("X-Forwarded-For") != null) {
                    request.remoteAddress = nettyRequest.getHeader("X-Forwarded-For");
                }
            }
        }


        addToRequest(nettyRequest, request);

        request.resolveFormat();

        request._init();

        return request;
    }

    protected static void addToRequest(HttpRequest nettyRequest, Request request) {
        for (String key : nettyRequest.getHeaderNames()) {
            Http.Header hd = new Http.Header();
            hd.name = key.toLowerCase();
            hd.values = new ArrayList<String>();
            for (String next : nettyRequest.getHeaders(key)) {
                hd.values.add(next);
            }
            request.headers.put(hd.name, hd);
        }

        String value = nettyRequest.getHeader("Cookie");
        if (value != null) {
            Set<Cookie> cookies = new CookieDecoder().decode(value);

            if (cookies != null) {
                for (Cookie cookie : cookies) {
                    Http.Cookie playCookie = new Http.Cookie();
                    playCookie.name = cookie.getName();
                    playCookie.path = cookie.getPath();
                    playCookie.domain = cookie.getDomain();
                    playCookie.secure = cookie.isSecure();
                    playCookie.value = cookie.getValue();
                    request.cookies.put(playCookie.name, playCookie);
                }
            }
        }
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }

    public static void serve404(NotFound e, ChannelHandlerContext ctx, Request request) {
        HttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
        nettyResponse.setHeader("Server", signature);

        nettyResponse.setHeader("Content-Type", "text/html");
        Map<String, Object> binding = getBindingForErrors(e, false);

        String format = Request.current().format;
        if (format == null || ("XMLHttpRequest".equals(request.headers.get("x-requested-with")) && "html".equals(format))) {
            format = "txt";
        }
        // TODO: is that correct? xxx.?
        nettyResponse.setHeader("Content-Type", (MimeTypes.getContentType("xxx." + format, "text/plain")));


        String errorHtml = TemplateLoader.load("errors/404." + format).render(binding);
        try {
            ChannelBuffer buf = ChannelBuffers.copiedBuffer(errorHtml.getBytes("utf-8"));
            nettyResponse.setContent(buf);
            ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
            writeFuture.addListener(ChannelFutureListener.CLOSE);
        } catch (UnsupportedEncodingException fex) {
            Logger.error(fex, "(utf-8 ?)");
        }
    }

    protected static Map<String, Object> getBindingForErrors(Exception e, boolean isError) {

        Map<String, Object> binding = new HashMap<String, Object>();
        if (!isError) {
            binding.put("result", e);
        } else {
            binding.put("exception", e);
        }
        binding.put("session", Scope.Session.current());
        binding.put("request", Http.Request.current());
        binding.put("flash", Scope.Flash.current());
        binding.put("params", Scope.Params.current());
        binding.put("play", new Play());
        try {
            binding.put("errors", Validation.errors());
        } catch (Exception ex) {
            Logger.error(ex, "Error when getting Validation errors");
        }
        return binding;
    }

    // TODO: add request and response as parameter
    public static void serve500(Exception e, ChannelHandlerContext ctx) {

        HttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        nettyResponse.setHeader("Server", signature);

        Request request = Request.current();
        Response response = Response.current();

        try {
            if (!(e instanceof PlayException)) {
                e = new play.exceptions.UnexpectedException(e);
            }

            // Flush some cookies
            try {
                CookieEncoder encoder = new CookieEncoder(true);
                Map<String, Http.Cookie> cookies = response.cookies;
                for (Http.Cookie cookie : cookies.values()) {
                    Cookie c = new DefaultCookie(cookie.name, cookie.value);
                    c.setSecure(cookie.secure);
                    c.setPath(cookie.path);
                    if (cookie.domain != null) {
                        c.setDomain(cookie.domain);
                    }
                    if (cookie.maxAge != null) {
                        c.setMaxAge(cookie.maxAge);
                    }
                    encoder.addCookie(c);
                }

                nettyResponse.setHeader("Cookie", encoder.encode());
            } catch (Exception exx) {
                Logger.error(e, "Trying to flush cookies");
                // humm ?
            }
            Map<String, Object> binding = getBindingForErrors(e, true);

            String format = request.format;
            if (format == null || ("XMLHttpRequest".equals(request.headers.get("x-requested-with")) && "html".equals(format))) {
                format = "txt";
            }

            // TODO: is that correct? xxx.?
            nettyResponse.setHeader("Content-Type", (MimeTypes.getContentType("xxx." + format, "text/plain")));
            try {
                String errorHtml = TemplateLoader.load("errors/500." + format).render(binding);

                ChannelBuffer buf = ChannelBuffers.copiedBuffer(errorHtml.getBytes("utf-8"));
                nettyResponse.setContent(buf);
                ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                writeFuture.addListener(ChannelFutureListener.CLOSE);
                Logger.error(e, "Internal Server Error (500) for request %s", request.method + " " + request.url);
            } catch (Throwable ex) {
                Logger.error(e, "Internal Server Error (500) for request %s", request.method + " " + request.url);
                Logger.error(ex, "Error during the 500 response generation");
                try {
                    ChannelBuffer buf = ChannelBuffers.copiedBuffer("Internal Error (check logs)".getBytes("utf-8"));
                    nettyResponse.setContent(buf);
                    ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                    writeFuture.addListener(ChannelFutureListener.CLOSE);
                } catch (UnsupportedEncodingException fex) {
                    Logger.error(fex, "(utf-8 ?)");
                }
            }
        } catch (Throwable exxx) {
            try {
                ChannelBuffer buf = ChannelBuffers.copiedBuffer("Internal Error (check logs)".getBytes("utf-8"));
                nettyResponse.setContent(buf);
                ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            } catch (Exception fex) {
                Logger.error(fex, "(utf-8 ?)");
            }
            if (exxx instanceof RuntimeException) {
                throw (RuntimeException) exxx;
            }
            throw new RuntimeException(exxx);
        }
    }

    public static void serveStatic(RenderStatic renderStatic, ChannelHandlerContext ctx, Request request, Response response, HttpRequest nettyRequest) {
        HttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        nettyResponse.setHeader("Server", signature);
        try {
            VirtualFile file = Play.getVirtualFile(renderStatic.file);
            if (file != null && file.exists() && file.isDirectory()) {
                file = file.child("index.html");
                if (file != null) {
                    renderStatic.file = file.relativePath();
                }
            }
            if ((file == null || !file.exists())) {
                serve404(new NotFound("The file " + renderStatic.file + " does not exist"), ctx, request);
            } else {
                boolean raw = false;
                for (PlayPlugin plugin : Play.plugins) {
                    if (plugin.serveStatic(file, Request.current(), Response.current())) {
                        raw = true;
                        break;
                    }
                }
                if (raw) {
                    copyResponse(ctx, request, response, nettyRequest);
                } else {
                    addEtag(request, nettyResponse, file.getRealFile());
                    nettyResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(file.length()));
                    ctx.getChannel().write(nettyResponse);
                    ChannelFuture writeFuture = ctx.getChannel().write(new ChunkedNioFile(file.getRealFile()));
                    // Decide whether to close the connection or not.
                    boolean close =
                            HttpHeaders.Values.CLOSE.equalsIgnoreCase(nettyRequest.getHeader(HttpHeaders.Names.CONNECTION)) ||
                                    nettyRequest.getProtocolVersion().equals(HttpVersion.HTTP_1_0) &&
                                            !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(nettyRequest.getHeader(HttpHeaders.Names.CONNECTION));

                    if (close) {
                        // Close the connection when the whole content is written out.
                        writeFuture.addListener(ChannelFutureListener.CLOSE);
                    }
                }

            }
        } catch (Exception e) {
            Logger.error(e, "serveStatic for request %s", request.method + " " + request.url);
            try {
                ChannelBuffer buf = ChannelBuffers.copiedBuffer("Internal Error (check logs)".getBytes("utf-8"));
                nettyResponse.setContent(buf);
                ChannelFuture future = ctx.getChannel().write(nettyResponse);
                future.addListener(ChannelFutureListener.CLOSE);
            } catch (Exception ex) {
                Logger.error(e, "serveStatic for request %s", request.method + " " + request.url);
            }
        }
    }

    public static boolean isModified(String etag, long last, Request request) {
        if (request.headers.containsKey("If-None-Match")) {
            final String browserEtag = request.headers.get("If-None-Match").value();
            if (browserEtag.equals(etag)) {
                return false;
            }
            return true;
        }

        if (request.headers.containsKey("If-Modified-Since")) {
            final String ifModifiedSince = request.headers.get("If-Modified-Since").value();

            if (!StringUtils.isEmpty(ifModifiedSince)) {
                try {
                    Date browserDate = Utils.getHttpDateFormatter().parse(ifModifiedSince);
                    if (browserDate.getTime() >= last) {
                        return false;
                    }
                } catch (ParseException ex) {
                    Logger.warn("Can't parse HTTP date", ex);
                }
                return true;
            }
        }
        return true;
    }

    private static void addEtag(Request request, HttpResponse minaResponse, File file) throws IOException {
        if (Play.mode == Play.Mode.DEV) {
            minaResponse.setHeader("Cache-Control", "no-cache");
        } else {
            String maxAge = Play.configuration.getProperty("http.cacheControl", "3600");
            if (maxAge.equals("0")) {
                minaResponse.setHeader("Cache-Control", "no-cache");
            } else {
                minaResponse.setHeader("Cache-Control", "max-age=" + maxAge);
            }
        }
        boolean useEtag = Play.configuration.getProperty("http.useETag", "true").equals("true");
        long last = file.lastModified();
        final String etag = "\"" + last + "-" + file.hashCode() + "\"";
        if (!isModified(etag, last, request)) {
            minaResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_MODIFIED);
            if (useEtag) {
                minaResponse.setHeader("Etag", etag);
            }

        } else {
            minaResponse.setHeader("Last-Modified", Utils.getHttpDateFormatter().format(new Date(last)));
            if (useEtag) {
                minaResponse.setHeader("Etag", etag);
            }
            minaResponse.setHeader("Content-Type", MimeTypes.getContentType(file.getName()));
            minaResponse.setHeader("Content-Length", "" + file.length());
        }
    }

}
