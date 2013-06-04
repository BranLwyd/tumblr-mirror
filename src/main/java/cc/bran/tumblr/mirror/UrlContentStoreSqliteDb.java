package cc.bran.tumblr.mirror;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Implements SQLite-based database for TumblrWatcher data.
 */
// TODO(bpitman): call this a key-value store
// TODO(bpitman): don't use SQLite for a key-value store
public class UrlContentStoreSqliteDb {

  private Connection dbConnection;

  public UrlContentStoreSqliteDb(String dbFile) throws ClassNotFoundException, SQLException {
    Class.forName("org.sqlite.JDBC");
    File file = new File(dbFile);
    dbConnection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", file.toString()));
    init();
  }

  public byte[] getContent(String url) throws SQLException {
    byte[] content = null;

    try (PreparedStatement selectStatement = dbConnection
        .prepareStatement("SELECT content FROM pages WHERE url = ?;")) {
      selectStatement.setString(1, url);
      try (ResultSet results = selectStatement.executeQuery()) {
        if (results.next()) {
          content = results.getBytes(1);
        }
      }
    }

    return content;
  }

  public boolean hasContent(String url) throws SQLException {
    try (PreparedStatement prepStatement = dbConnection
        .prepareStatement("SELECT count(*) FROM pages WHERE url = ?;")) {
      prepStatement.setString(1, url);
      try (ResultSet results = prepStatement.executeQuery()) {
        if (results.next()) {
          int rows = results.getInt(1);
          return (rows > 0);
        }
      }
    }

    return false;
  }

  private void init() throws SQLException {
    try (Statement statement = dbConnection.createStatement()) {
      statement
          .executeUpdate("CREATE TABLE IF NOT EXISTS pages(id INTEGER PRIMARY KEY ASC AUTOINCREMENT, url STRING UNIQUE NOT NULL, content BLOB);");
      statement.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS pageUrlIndex ON pages(url);");
    }
  }

  public void setContent(String url, byte[] content) throws SQLException {
    try (Statement statement = dbConnection.createStatement();
        PreparedStatement updateStatement = dbConnection
            .prepareStatement("UPDATE pages SET content = ? WHERE url = ?;");
        PreparedStatement insertStatement = dbConnection
            .prepareStatement("INSERT INTO pages (url, content) VALUES (?, ?);")) {
      statement.execute("BEGIN TRANSACTION;");
      try {
        updateStatement.setBytes(1, content);
        updateStatement.setString(2, url);
        int rowsAffected = updateStatement.executeUpdate();

        // TODO(bpitman): debug!
        if (rowsAffected != 0) {
          throw new Error("setContent called twice for " + url + "!!!");
        }

        if (rowsAffected == 0) {
          insertStatement.setString(1, url);
          insertStatement.setBytes(2, content);
          insertStatement.execute();
        }
      } finally {
        statement.execute("COMMIT TRANSACTION;");
      }
    }
  }
}
