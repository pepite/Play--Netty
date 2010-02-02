package play.modules.netty;

import org.apache.asyncweb.common.HttpHeaderConstants;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.stream.ChunkedNioFile;
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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.text.ParseException;
import java.util.*;


@ChannelPipelineCoverage("one")
public class PlayHandler extends SimpleChannelUpstreamHandler {

    private final static String signature = "Play! Framework;" + Play.version + ";" + Play.mode.name().toLowerCase();

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Object msg = e.getMessage();
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
                copyResponse(ctx, response);
            } else {
                Invoker.invoke(new NettyInvocation(request, response, ctx));
            }
        } catch (Exception ex) {
            serve500(ex, ctx);
            return;
        }
    }

    private static Map<String, RenderStatic> staticPathsCache = new HashMap();

    public class NettyInvocation extends Invoker.DirectInvocation {


        private final ChannelHandlerContext ctx;
        private final Request request;
        private final Response response;

        public NettyInvocation(Request request, Response response, ChannelHandlerContext ctx) {
            this.ctx = ctx;
            this.request = request;
            this.response = response;
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
                synchronized (staticPathsCache) {
                    serveStatic(staticPathsCache.get(request.path), ctx, request, response);
                }
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
                serveStatic(e, ctx, request, response);
                return false;
            }
            return true;
        }

        @Override
        public void run() {
            try {
                super.run();
            } catch (Exception e) {
                serve500(e, ctx);
            }
        }

        @Override
        public void execute() throws Exception {
            ActionInvoker.invoke(request, response);
            copyResponse(ctx, response);
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

    public final static Object lock = new Object();

    protected static void writeResponse(ChannelHandlerContext ctx, Response response, HttpResponse nettyResponse) {
        ChannelBuffer buf = ChannelBuffers.copiedBuffer(response.out.toByteArray());
        nettyResponse.setContent(buf);

        synchronized (lock) {
            ChannelFuture f = ctx.getChannel().write(nettyResponse);
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public static void copyResponse(ChannelHandlerContext ctx, Response response) throws Exception {
        response.out.flush();
        HttpResponse nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(response.status));
        nettyResponse.setHeader("Server", signature);

        if (response.contentType != null) {
            nettyResponse.setHeader("Content-Type", response.contentType + (response.contentType.startsWith("text/") && !response.contentType.contains("charset") ? "; charset=utf-8" : ""));
        } else {
            nettyResponse.setHeader("Content-Type", "text/plain; charset=utf-8");
        }

        addToResponse(response, nettyResponse);

        if ((response.direct != null) && response.direct.isFile()) {
            nettyResponse.setHeader("Content-Length", "" + response.direct.length());

            try {
                nettyResponse.setHeader(HttpHeaderConstants.KEY_CONTENT_LENGTH, "" + response.direct.length());

                nettyResponse.setHeader("Content-Type", MimeTypes.getContentType(response.direct.getName()));

                synchronized (lock) {
                    ChannelFuture future = ctx.getChannel().write(nettyResponse);
                    future.addListener(ChannelFutureListener.CLOSE);
                    ChannelFuture writeFuture = ctx.getChannel().write(new ChunkedNioFile(response.direct));
                    writeFuture.addListener(ChannelFutureListener.CLOSE);
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        } else {
            writeResponse(ctx, response, nettyResponse);
        }

    }

    public static Request parseRequest(ChannelHandlerContext ctx, HttpRequest nettyRequest) throws Exception {
        final URI uri = new URI(nettyRequest.getUri());

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

        request.body = new ChannelBufferInputStream(nettyRequest.getContent().duplicate());
        request.url = uri.toString();
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
        Map<String, Object> binding = getBindingForErrors(e);

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
            synchronized (lock) {
                ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (UnsupportedEncodingException fex) {
            Logger.error(fex, "(utf-8 ?)");
        }
    }

    protected static Map<String, Object> getBindingForErrors(Exception e) {

        Map<String, Object> binding = new HashMap<String, Object>();
        binding.put("result", e);
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
            Map<String, Object> binding = getBindingForErrors(e);

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
                synchronized (lock) {
                    ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                    writeFuture.addListener(ChannelFutureListener.CLOSE);
                }
                Logger.error(e, "Internal Server Error (500) for request %s", request.method + " " + request.url);
            } catch (Throwable ex) {
                Logger.error(e, "Internal Server Error (500) for request %s", request.method + " " + request.url);
                Logger.error(ex, "Error during the 500 response generation");
                try {
                    ChannelBuffer buf = ChannelBuffers.copiedBuffer("Internal Error (check logs)".getBytes("utf-8"));
                    nettyResponse.setContent(buf);
                    synchronized (lock) {
                        ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                        writeFuture.addListener(ChannelFutureListener.CLOSE);
                    }
                } catch (UnsupportedEncodingException fex) {
                    Logger.error(fex, "(utf-8 ?)");
                }
            }
        } catch (Throwable exxx) {
            try {
                ChannelBuffer buf = ChannelBuffers.copiedBuffer("Internal Error (check logs)".getBytes("utf-8"));
                nettyResponse.setContent(buf);
                synchronized (lock) {
                    ChannelFuture writeFuture = ctx.getChannel().write(nettyResponse);
                    writeFuture.addListener(ChannelFutureListener.CLOSE);
                }
            } catch (Exception fex) {
                Logger.error(fex, "(utf-8 ?)");
            }
            if (exxx instanceof RuntimeException) {
                throw (RuntimeException) exxx;
            }
            throw new RuntimeException(exxx);
        }
    }

    public static void serveStatic(RenderStatic renderStatic, ChannelHandlerContext ctx, Request request, Response response) {
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
                    copyResponse(ctx, response);
                } else {
                    if (Play.mode == Play.Mode.DEV) {
                        nettyResponse.setHeader("Cache-Control", "no-cache");
                    } else {
                        String maxAge = Play.configuration.getProperty("http.cacheControl", "3600");
                        if (maxAge.equals("0")) {
                            nettyResponse.setHeader("Cache-Control", "no-cache");
                        } else {
                            nettyResponse.setHeader("Cache-Control", "max-age=" + maxAge);
                        }
                    }
                    boolean useEtag = Play.configuration.getProperty("http.useETag", "true").equals("true");
                    long last = file.lastModified();
                    String etag = "\"" + last + "-" + file.hashCode() + "\"";
                    if (!isModified(etag, last, request)) {
                        if (useEtag) {
                            nettyResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_MODIFIED);
                            nettyResponse.setHeader("Etag", etag);
                        }
                    } else {
                        nettyResponse.setHeader("Last-Modified", Utils.getHttpDateFormatter().format(new Date(last)));
                        if (useEtag) {
                            nettyResponse.setHeader("Etag", etag);
                        }


                        nettyResponse.setHeader("Content-Type", MimeTypes.getContentType(file.getName()));
                        nettyResponse.setHeader("Content-Length", "" + file.length());

                        synchronized (lock) {
                            ChannelFuture future = ctx.getChannel().write(nettyResponse);
                            future.addListener(ChannelFutureListener.CLOSE);
                        }
                        synchronized (lock) {
                            ChannelFuture writeFuture = ctx.getChannel().write(new ChunkedNioFile(file.getRealFile()));
                            writeFuture.addListener(ChannelFutureListener.CLOSE);
                        }
                    }
                }

            }
        } catch (Exception e) {
            Logger.error(e, "serveStatic for request %s", request.method + " " + request.url);
            try {
                ChannelBuffer buf = ChannelBuffers.copiedBuffer("Internal Error (check logs)".getBytes("utf-8"));
                nettyResponse.setContent(buf);
                synchronized (lock) {
                    ChannelFuture future = ctx.getChannel().write(nettyResponse);
                    future.addListener(ChannelFutureListener.CLOSE);
                }
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


}
