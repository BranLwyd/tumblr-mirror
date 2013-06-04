package cc.bran.tumblr.mirror;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class TumblrMirrorCli {

  private static final long DEFAULT_MS_PER_REQUEST = 1000 * 5; // 5 seconds

  // This is a main class.
  private TumblrMirrorCli() {}

  public static void main(String[] args) {
    // Prepare to parse command line arguments.
    Option tumblrNameOption = OptionBuilder.withArgName("tumblr_name").hasArg().isRequired()
        .withDescription("name of tumblr to mirror").withLongOpt("tumblr_name").create();
    Option dbFileOption = OptionBuilder.withArgName("db_file").hasArg().isRequired()
        .withDescription("database file to use").withLongOpt("db_file").create();
    Option msPerRequestOption = OptionBuilder.withArgName("request_time").hasArg()
        .withDescription("milliseconds between network requests").withLongOpt("request_time")
        .create();

    Options options = new Options();
    options.addOption(tumblrNameOption);
    options.addOption(dbFileOption);
    options.addOption(msPerRequestOption);

    // Parse arguments.
    CommandLineParser parser = new PosixParser();
    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException ex) {
      System.err.println(ex.getMessage());
      System.exit(1);
    }

    String tumblrName = commandLine.getOptionValue("tumblr_name");
    String dbFile = commandLine.getOptionValue("db_file");
    long msPerRequest = DEFAULT_MS_PER_REQUEST;

    if (commandLine.hasOption("request_time")) {
      String requestTimeString = commandLine.getOptionValue("request_time");

      try {
        msPerRequest = Long.parseLong(requestTimeString);
      } catch (NumberFormatException ex) {
        System.err.println("--request_time should be a number.");
        System.exit(1);
      }
    }

    // Start mirroring.
    TumblrWatcher watcher = new TumblrWatcher(tumblrName, msPerRequest, dbFile);
    watcher.run();
  }
}
