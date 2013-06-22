package cc.bran.tumblr.mirror;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.RateLimiter;

public class TumblrWatcher implements Runnable {

  /**
   * Info from robots.txt.
   * 
   * This class supports only what is needed for tumblr's robots.txt files; it
   * is not a general-purpose robots.txt parser.
   */
  private static class RobotsInfo {

    private final List<String> disallowedPrefixes;

    private final List<String> sitemapUrls;

    public RobotsInfo(List<String> sitemapUrls, List<String> disallowedPrefixes) {
      this.sitemapUrls = sitemapUrls;
      this.disallowedPrefixes = disallowedPrefixes;
    }

    public boolean checkUrl(String url) {
      String path;
      try {
        path = new URL(url).getPath();
      } catch (MalformedURLException exception) {
        logger.log(Level.WARNING, "malformed URL passed to checkUrl", exception);
        return false;
      }

      for (String disallowedPrefix : disallowedPrefixes) {
        if (path.startsWith(disallowedPrefix)) {
          return false;
        }
      }

      return true;
    }

    public List<String> getDisallowedPrefixes() {
      return Collections.unmodifiableList(disallowedPrefixes);
    }

    public List<String> getSitemapUrls() {
      return Collections.unmodifiableList(sitemapUrls);
    }
  }

  private static final DocumentBuilder docBuilder;

  private static final XPathExpression LOC_SET_EXPRESSION;

  private static final Logger logger;

  private static final Pattern POST_PATTERN = Pattern.compile("/post/(\\d+)");

  private static final String USER_AGENT = "TumblrMirror in-dev";

  static {
    logger = Logger.getLogger(TumblrWatcher.class.getCanonicalName());

    try {
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      docBuilderFactory.setNamespaceAware(true);
      docBuilder = docBuilderFactory.newDocumentBuilder();

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xPath = xPathFactory.newXPath();
      xPath.setNamespaceContext(new NamespaceContext() {
        @Override
        public String getNamespaceURI(String prefix) {
          if ("sm".equals(prefix)) {
            return "http://www.sitemaps.org/schemas/sitemap/0.9";
          }

          return XMLConstants.NULL_NS_URI;
        }

        @Override
        public String getPrefix(String namespaceURI) {
          return null;
        }

        @Override
        public Iterator getPrefixes(String namespaceURI) {
          return null;
        }
      });

      LOC_SET_EXPRESSION = xPath.compile("//sm:loc/text()");
    } catch (Exception exception) {
      logger.log(Level.SEVERE, "unhandled exception in static initializer!", exception);
      throw new ExceptionInInitializerError(exception); // this kills the
                                                        // program
    }
  }

  private UrlContentStoreSqliteDb contentStore;

  private final String dbFile;

  private final RateLimiter requestLimiter;

  private final String tumblrName;

  public TumblrWatcher(String tumblrName, long msPerRequest, String dbFile) {
    this.tumblrName = tumblrName;
    this.requestLimiter = RateLimiter.create(1000.0 / msPerRequest);
    this.dbFile = dbFile;
  }

  private String canonicalize(String urlString) {
    try {
      // Convert %20 into dashes, as tumblr treats these as the same.
      urlString = urlString.replace("%20", "-");

      // Remove descriptive text and other junk from post URLs.
      URL url = new URL(urlString);
      String path = url.getPath();
      Matcher postMatcher = POST_PATTERN.matcher(path);
      if (postMatcher.lookingAt()) {
        String postId = postMatcher.group(1);
        return String.format("http://%s/post/%s", url.getAuthority(), postId);
      }

      // Remove trailing # & query from any URL.
      return String.format("http://%s%s", url.getAuthority(), path);
    } catch (MalformedURLException exception) {
      logMessage(Level.WARNING, "malformed URL while attempting canonicalization.", exception);
      return urlString;
    }
  }

  private void downloadPages(Multimap<String, String> knownPages, RobotsInfo robotsInfo) {
    logMessage(Level.INFO, "downloading pages.");

    Queue<String> workQueue = new ArrayDeque<String>(knownPages.keySet());

    String pageUrl;
    while ((pageUrl = workQueue.poll()) != null) {
      URLConnection connection = null;
      InputStream contentStream = null;

      if (!robotsInfo.checkUrl(pageUrl)) {
        logMessage(Level.INFO, String.format("ignoring %s due to robots.txt.", pageUrl.toString()));
        continue;
      }

      try {
        try {
          connection = getContentStream(pageUrl);
          contentStream = connection.getInputStream();
        } catch (IOException ex) {
          logMessage(
              Level.WARNING,
              String.format("problem retrieving content. (linked from %s)",
                  Joiner.on(", ").join(knownPages.get(pageUrl))), ex);
          continue;
        }

        // Read content.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
          byte[] buffer = new byte[1024];
          int read;

          while ((read = contentStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, read);
          }
        } catch (IOException ex) {
          logMessage(Level.WARNING, "error reading content.", ex);
          continue;
        }

        // Save content to DB.
        byte[] content = outputStream.toByteArray();
        try {
          contentStore.setContent(pageUrl, content);
        } catch (SQLException ex) {
          logMessage(Level.WARNING, "error updating page database.", ex);
        }

        // Parse content for additional links.
        String contentType = connection.getHeaderField("Content-Type");
        if (contentType != null && contentType.toLowerCase().contains("text/html")) {
          Set<String> linksInPage;
          String pageAuthority;

          try {
            linksInPage = parseLinks(pageUrl, content);
          } catch (IOException exception) {
            logMessage(Level.WARNING, "error while parsing page for links.", exception);
            continue;
          }

          try {
            pageAuthority = new URL(pageUrl).getAuthority();
          } catch (MalformedURLException exception) {
            logMessage(Level.WARNING, "error getting authority for page", exception);
            continue;
          }

          for (String linkUrl : linksInPage) {
            String linkAuthority;
            try {
              linkAuthority = new URL(linkUrl).getAuthority();
            } catch (MalformedURLException exception) {
              logMessage(Level.WARNING, "error getting authority for page", exception);
              continue;
            }

            if (!pageAuthority.equals(linkAuthority)) {
              continue;
            }

            if (!knownPages.containsKey(linkUrl)) {
              logMessage(Level.INFO, String.format("queueing %s for download.", linkUrl));
              workQueue.offer(linkUrl);
            }

            knownPages.put(linkUrl, pageUrl);
          }
        }
      } finally {
        try {
          if (contentStream != null) {
            contentStream.close();
          }
        } catch (IOException ex) {
          logMessage(Level.WARNING, "error closing page content stream.", ex);
        }
      }
    }
  }

  private URLConnection getContentStream(String url) throws IOException {
    requestLimiter.acquire();
    logMessage(Level.INFO, String.format("retrieving %s.", url));
    URLConnection connection = new URL(url).openConnection();
    connection.setRequestProperty("User-Agent", USER_AGENT);
    connection.connect();
    return connection;
  }

  private void logMessage(Level level, String message) {
    logMessage(level, message, null);
  }

  private void logMessage(Level level, String message, Throwable throwable) {
    String realMessage = new StringBuilder().append("[").append(tumblrName).append("] ")
        .append(message).toString();
    logger.log(level, realMessage, throwable);
  }

  private Set<String> parseLinks(String url, byte[] content) throws IOException {
    logMessage(Level.INFO, "parsing page for links...");

    Set<String> urls = new HashSet<String>();
    Document document = Jsoup.parse(new ByteArrayInputStream(content), null, url);

    // Anchors.
    Elements links = document.select("a[href]");
    for (Element elem : links) {
      urls.add(canonicalize(elem.attr("abs:href")));
    }

    // Imports.
    Elements imports = document.select("link[href]");
    for (Element elem : imports) {
      urls.add(canonicalize(elem.attr("abs:href")));
    }

    // Media.
    Elements media = document.select("[src]");
    for (Element elem : media) {
      urls.add(canonicalize(elem.attr("abs:src")));
    }

    return urls;
  }

  private RobotsInfo readRobots(String robotsUrl) throws IOException {
    List<String> sitemapUrls = new ArrayList<String>();
    List<String> disallowedPrefixes = new ArrayList<String>();

    logMessage(Level.INFO, "parsing robots.txt...");

    try (InputStream contentStream = getContentStream(robotsUrl).getInputStream()) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(contentStream));
      String line;

      while ((line = reader.readLine()) != null) {
        if (line.startsWith("Sitemap: ")) {
          String sitemapUrl = line.substring("Sitemap: ".length());
          sitemapUrls.add(sitemapUrl);
        } else if (line.startsWith("Disallow: ")) {
          String disallowedPrefix = line.substring("Disallow: ".length());
          disallowedPrefixes.add(disallowedPrefix);
        }
      }
    }

    return new RobotsInfo(sitemapUrls, disallowedPrefixes);
  }

  private Multimap<String, String> readSitemaps(RobotsInfo robotsInfo) {
    Multimap<String, String> pagesInSitemap = HashMultimap.create();

    logMessage(Level.INFO, "downloading site maps.");

    for (String sitemapUrl : robotsInfo.getSitemapUrls()) {
      InputStream contentStream = null;

      try {
        // Read content.
        try {
          contentStream = getContentStream(sitemapUrl).getInputStream();
        } catch (IOException ex) {
          logMessage(Level.WARNING, "problem retrieving sitemap content.", ex);
          break;
        }

        // Parse XML.
        org.w3c.dom.Document sitemapDoc;
        try {
          sitemapDoc = docBuilder.parse(contentStream);
        } catch (IOException ex) {
          logMessage(Level.WARNING, "problem reading content.", ex);
          continue;
        } catch (SAXException ex) {
          logMessage(Level.WARNING, "problem parsing content.", ex);
          continue;
        }

        // Find update information from XML document.
        NodeList locNodes;
        try {
          locNodes = (NodeList) LOC_SET_EXPRESSION.evaluate(sitemapDoc, XPathConstants.NODESET);
        } catch (XPathExpressionException ex) {
          logMessage(Level.WARNING, "problem evaluating XPath expression.", ex);
          continue;
        }
        for (int i = 0; i < locNodes.getLength(); i++) {
          String locString = locNodes.item(i).getTextContent();

          pagesInSitemap.put(canonicalize(locString), sitemapUrl);
        }
      } finally {
        try {
          if (contentStream != null) {
            contentStream.close();
          }
        } catch (IOException ex) {
          logMessage(Level.WARNING, "error closing sitemap content stream.", ex);
        }
      }
    }

    return pagesInSitemap;
  }

  @Override
  public void run() {
    logMessage(Level.INFO, "starting up.");

    try {
      logMessage(Level.INFO, "getting DB connection.");
      try {
        contentStore = new UrlContentStoreSqliteDb(dbFile);
      } catch (SQLException ex) {
        logMessage(Level.SEVERE, "could not get DB connection!", ex);
        return;
      }

      RobotsInfo robotsInfo;
      try {
        String robotsUrl = String.format("http://%s.tumblr.com/robots.txt", tumblrName);
        robotsInfo = readRobots(robotsUrl);
      } catch (IOException exception) {
        logMessage(Level.SEVERE, "could not read robots.txt!", exception);
        return;
      }

      logMessage(Level.INFO, "starting update sequence.");
      Multimap<String, String> pagesInSitemap = readSitemaps(robotsInfo);
      downloadPages(pagesInSitemap, robotsInfo);
      logMessage(Level.INFO, "update sequence complete.");
    } catch (Exception ex) {
      logMessage(Level.SEVERE, "uncaught exception at top level!", ex);
    } finally {
      logMessage(Level.INFO, "shutting down.");
    }
  }
}
