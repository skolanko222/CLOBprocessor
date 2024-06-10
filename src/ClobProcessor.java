import com.microsoft.sqlserver.jdbc.SQLServerException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.*;

public class ClobProcessor {

    private Connection connection;
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    private String dbName;
    private String tbName;
    private String ftCatalogName;
    private String ftIndexName = "FT_ID";
    private String primaryKeyname = "ID";
    private boolean createTable = true;
    private String language = "english";
    static public final Logger logger = new Logger("ClobProcessorLogger", null) {};
    public void setTbName(String tbName) {
        this.tbName = tbName;
    }
    public void setPrimaryKeyname(String primaryKeyname) {
        this.primaryKeyname = primaryKeyname;
        this.ftIndexName = "FT_" + primaryKeyname;
    }
    public void setLanguage(String language) {
        this.language = language;
    }
    private String getLanguageCode(String language) {
        return switch (language) {
            case "polish" -> "1045";
            case "german" -> "1031";
            case "french" -> "1036";
            default -> "1033"; // English
        };
    }
    public void setCreateTableFlag(boolean createTable) {this.createTable = createTable;}

    static {
        try {
            //FileHandler file name with max size and number of log files limit
            Handler fileHandler = new FileHandler("./logger.log", 2000, 5);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
        logger.log(Level.INFO, "Logger initialized");
        } catch (SecurityException | IOException e) {
            e.printStackTrace();
    }
        logger.addHandler(new ConsoleHandler());}
    // Constructor that initializes the connection to the database using the url
    public ClobProcessor(String url) {
        try{
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Connection to the database failed");
        }
    }
    // Constructor that initializes the connection to the database using the connection object
    public ClobProcessor(Connection connection) {
        this.connection = connection;
    }
    // Check if the database exists
    public boolean checkIfDBExists(String dbName) throws SQLException {
        String sql = "SELECT name FROM master.dbo.sysdatabases WHERE name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, dbName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    // Check if the table exists
    public boolean checkIfTableExists(String dbName, String tbName) throws SQLException {
        String sql = "USE " + dbName + "; SELECT name FROM sys.tables WHERE name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tbName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
            }
        }
        // Check if the table has the correct primary key
        sql = "USE " + dbName + "; SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tbName);
            pstmt.setString(2, primaryKeyname);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (!rs.next()) {
                    logger.log(Level.WARNING, "Table does not have the correct primary key");
                    throw new SQLException("Tabela istnieje, ale nie ma poprawnego klucza głównego");
                }
                else {
                    return true;
                }
            }
        }

    }
    // Check if the document exists inside the "tbName" table
    public boolean checkIfDocumentExists(Integer ID)  {
        String sql = "USE " + dbName + "; SELECT " + primaryKeyname + " FROM " + tbName + " WHERE " + primaryKeyname + " = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, ID.toString());
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
            catch(SQLServerException e) {
                logger.log(Level.SEVERE, "Document existence check failed");
                return false;
            }
        }
        catch (SQLException e) {
            logger.log(Level.SEVERE, "Document existence check failed");
            return false;
        }
    }

    // Check if the full text catalog exists
    public boolean checkIfFtCatalogExists(String dbName, String ftCatalogName) throws SQLException {
        String sql = "USE " + dbName + "; SELECT name FROM sys.fulltext_catalogs WHERE name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, ftCatalogName );
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    // Check if the full text index exists on given table
    public boolean checkIfFtIndexExists(String dbName, String tbName) throws SQLException {
        String sql = "USE " + dbName + "; SELECT * FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID(?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tbName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    // Initialize the database, create full text catalog and full text index on the table in given
    // database, or create the database and table if they do not exist
    public void initDatabase() throws SQLException {
        String sql;
        if(!checkIfDBExists(dbName)) {
            sql = "CREATE DATABASE " + dbName + ";";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                logger.log(Level.SEVERE, "Database creation failed");
                throw e;
            }
        }
        else {
            logger.log(Level.INFO, "Database already exists");
        }
        logger.log(Level.INFO, "Database initialized");

        //ensure that the table has a unique, single-column, non-nullable index.
        ftIndexName += "_" + tbName.toUpperCase();
        sql = "USE " + dbName + "; CREATE TABLE " + tbName +" ("+ primaryKeyname + " INT IDENTITY(1,1) NOT NULL CONSTRAINT " + ftIndexName + " PRIMARY KEY, Content NVARCHAR(MAX) );";

        if(!checkIfTableExists(dbName, tbName)) {
            if (!createTable) {
                throw new SQLException("Nie można połączyć, bo tabela nie istnieje!");
            }
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                logger.log(Level.SEVERE, "Table creation failed");
                throw e;
            }
        }
        else {
            logger.log(Level.INFO, "Table already exists");
            if(createTable) {
                throw new SQLException("Nie można utworzyć tabeli, ponieważ już istnieje!");
            }
        }
        logger.log(Level.INFO, "Table initialized");
        // create full text CATALOG
        ftCatalogName = tbName + "_FTC";

        if(!checkIfFtCatalogExists(dbName, ftCatalogName)) {
            sql = "USE " + dbName + "; CREATE FULLTEXT CATALOG " + ftCatalogName + " AS DEFAULT " ;
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                logger.log(Level.SEVERE, "Full text catalog creation failed");
                throw e;
            }
        }
        else {
            logger.log(Level.INFO, "Full text catalog already exists");
        }
        logger.log(Level.INFO, "Full text catalog initialized");
        // create full text index
        if(!checkIfFtIndexExists(dbName, tbName)) {
            sql = "USE " + dbName + "; CREATE FULLTEXT INDEX ON " + tbName + "(Content LANGUAGE " + getLanguageCode(language) + " ) KEY INDEX " + ftIndexName + " ON " + ftCatalogName + ";";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                logger.log(Level.SEVERE, "Full text index creation failed");
                throw e;
            }
        }
        else {
            logger.log(Level.INFO, "Full text index already exists");
        }
        logger.log(Level.INFO, "Full text index initialized");
    }
    // Close the connection to the database
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
    // Save the document to the database, if ID is null, a new ID is generated, otherwise the document is saved with the given ID
    public Integer saveDocument(Integer ID, String documentContent) throws SQLException {
        String sql = "USE " + dbName + ";";
        try {
            PreparedStatement pstmt;
            if(ID != null) {
                if(checkIfDocumentExists(ID)) {
                    logger.log(Level.WARNING, "Document with this ID already exists");
                    throw new SQLException("Document with this ID already exists");
                }
                sql += "SET IDENTITY_INSERT " + tbName + " ON;"; // Enable inserting ID
                sql += "INSERT INTO " + tbName + " (" + primaryKeyname + ", Content) VALUES (?, ?);";
                sql += "SET IDENTITY_INSERT " + tbName + " OFF;";
                pstmt = connection.prepareStatement(sql);
                pstmt.setString(1, ID.toString());
                pstmt.setString(2, documentContent);
                pstmt.executeUpdate();
                return ID;
            }
            else {
                int newID = 0;
                sql += "INSERT INTO " + tbName + " (Content) OUTPUT INSERTED." + primaryKeyname + " VALUES (?);";
                pstmt = connection.prepareStatement(sql);
                try (PreparedStatement pstmt1 = connection.prepareStatement(sql)) {
                    pstmt.setString(1, documentContent);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            newID = rs.getInt(1);
                        }
                    }
                }
//                logger.log(Level.INFO, "Document saved with ID: " + newID);
                return newID;
            }
        }
        catch (SQLServerException e) {
            logger.log(Level.SEVERE, "Document insertion failed");
            return null;
        }
    }
    // Save the document to the database from the file, if ID is null, a new ID is generated, otherwise the document is saved with the given ID
    // This function stores whole content of the file into one row of the table
    public Integer saveDocumentFromFile(Integer id, String path) throws SQLException {
        String content = readTextFile(path);
        content = new String(content.getBytes(), StandardCharsets.UTF_8);
        try {
            return saveDocument(id, content);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to insert the document from the file");
            throw e;
        }
    }
    // Save the document into the database from the file,
    // Puts each line of the file into a separate row of the table
    public Integer saveDocumentFromFileByLine(String path) throws SQLException {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            Integer id = null;
            while ((line = br.readLine()) != null) {
                //line = new String(line.getBytes(), StandardCharsets.UTF_8);
                id = saveDocument(null, line);
            }
            return id;
        }
        catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to open the file");
            return null;
        }
        catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to insert the document from the file (line by line)");
            throw e;
        }

    }
    // Delete the document with the given ID
    public boolean deleteDocument(Integer ID) throws SQLException {
        String sql = "USE " + dbName + ";";
        sql += "DELETE FROM " + tbName + " WHERE " + primaryKeyname + "  = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, ID.toString());
            pstmt.executeUpdate();
            logger.log(Level.INFO, "Document deleted");
            return true;
        }
        catch (SQLServerException e) {
            logger.log(Level.SEVERE, "Document deletion failed");
            throw e;
        }
    }
    public void dropDatabase() throws SQLException {
        String sql = "USE master; DROP DATABASE " + dbName;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }
        catch (SQLServerException e) {
            logger.log(Level.INFO, "Database deletion failed");
            throw e;
        }
    }
    // Search for a document that contains the searchTerm
    // Returns the ArrayList of IDs of the documents that contain the searchTerm
    public ArrayList<String> searchContainDocument(String searchTerm) throws SQLException {
        String sql = "SELECT " + primaryKeyname + ", Content FROM " + tbName + " WHERE CONTAINS(Content, ?)";
        return getFtsResults(searchTerm, sql);
    }
    // Search for a document that contains the searchTerm:
    // searches for all documents that contain words related to searchTerm
    public ArrayList<String> searchFreeTextDocument(String searchTerm) throws SQLException {
        String sql = "SELECT " + primaryKeyname + ", Content FROM " + tbName + " WHERE FREETEXT(Content, ?)";
        return getFtsResults(searchTerm, sql);
    }

    private ArrayList<String> getFtsResults(String searchTerm, String sql) throws SQLException {
        ArrayList<String> results = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, searchTerm);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rs.getString(primaryKeyname));
                }
            }
        }
        return results;
    }

    //function to read a text file
    public String readTextFile(String path) {
        try {
            FileInputStream fileInputStream = new FileInputStream(path);
            byte[] data = new byte[fileInputStream.available()];
            if(fileInputStream.read(data) == -1) {
                logger.log(Level.SEVERE, "Failed to read the content of the file");
                return null;
            }
            return new String(data);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to open the file");
            return null;
        }
    }
}
