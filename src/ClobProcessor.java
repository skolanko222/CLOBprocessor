import com.microsoft.sqlserver.jdbc.SQLServerException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ClobProcessor {
    public String getDbName() {
        return dbName;
    }

    private Connection connection;

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    private String dbName;

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    private String tbName;
    private String ftCatalogName;
    private String ftIndexName = "FT_ID";
    
    public ClobProcessor(String url) {
        try{
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println("Connection Failed!");
        }
    }
    public ClobProcessor(Connection connection) {
        this.connection = connection;
    }
    private boolean checkIfDBExists(String dbName) throws SQLException {
        String sql = "SELECT name FROM master.dbo.sysdatabases WHERE name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, dbName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    private boolean checkIfTableExists(String dbName, String tbName) throws SQLException {
        String sql = "USE " + dbName + "; SELECT name FROM sys.tables WHERE name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tbName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    private boolean checkIfDocumentExists(Integer ID) throws SQLException {
        String sql = "USE " + dbName + "; SELECT ID FROM documents WHERE ID = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, ID.toString());
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    private boolean checkIfFtCatalogExists(String dbName, String ftCatalogName) throws SQLException {
        String sql = "USE " + dbName + "; SELECT name FROM sys.fulltext_catalogs WHERE name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, ftCatalogName );
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    private boolean checkIfFtIndexExists(String dbName, String tbName) throws SQLException {
        String sql = "USE " + dbName + "; SELECT * FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID(?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tbName);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    public void initDatabase() throws SQLException {
        String sql;
        if(!checkIfDBExists(dbName)) {
            sql = "CREATE DATABASE " + dbName + ";";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                System.out.println("Database creation failed");
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Database already exists");
        }

        sql = "USE " + dbName + "; CREATE TABLE " + tbName +" (ID INT IDENTITY(1,1) NOT NULL CONSTRAINT " + ftIndexName + " PRIMARY KEY, Content NVARCHAR(MAX) );";

        if(!checkIfTableExists(dbName, tbName)) {
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                System.out.println("Table creation failed");
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Table already exists");
        }
        // create full text CATALOG
        ftCatalogName = tbName + "_FTC";

        if(!checkIfFtCatalogExists(dbName, ftCatalogName)) {
            sql = "USE " + dbName + "; CREATE FULLTEXT CATALOG " + ftCatalogName + " AS DEFAULT;";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                System.out.println("Full text catalog creation failed");
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Full text catalog already exists");
        }
        // create full text index
        if(!checkIfFtIndexExists(dbName, tbName)) {
            sql = "USE " + dbName + "; CREATE FULLTEXT INDEX ON " + tbName + "(Content) KEY INDEX " + ftIndexName + " ON " + ftCatalogName + ";";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            catch (SQLServerException e) {
                System.out.println("Full text index creation failed");
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Full text index already exists");
        }
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    public void saveDocument(Integer ID, String documentContent) throws SQLException {
        String sql = "USE " + dbName + ";";
        try {
            PreparedStatement pstmt;
            if(ID != null) {
                sql += "SET IDENTITY_INSERT " + tbName + " ON;";
                sql += "INSERT INTO Documents (ID, Content) VALUES (?, ?);";
                sql += "SET IDENTITY_INSERT " + tbName + " OFF;";
                pstmt = connection.prepareStatement(sql);
                pstmt.setString(1, ID.toString());
                pstmt.setString(2, documentContent);
            }
            else {
                sql += "INSERT INTO " + tbName + " (Content) VALUES (?);";
                pstmt = connection.prepareStatement(sql);
                pstmt.setString(1, documentContent);
            }
            pstmt.executeUpdate();
        }
        catch (SQLServerException e) {
            System.out.println("Document insertion failed");
            e.printStackTrace();
        }
    }

    public void deleteDocument(Integer ID) throws SQLException {
        String sql = "USE " + dbName + ";";
        sql += "DELETE FROM documents WHERE ID = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, ID.toString());
            pstmt.executeUpdate();
        }
    }

    public void searchDocument(String searchTerm) throws SQLException {
        String sql = "SELECT ID, Content FROM documents WHERE CONTAINS(Content, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, searchTerm);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String ID = rs.getString("ID");
                    String Content = rs.getString("Content");
                    System.out.println("Document Name: " + ID);
                    System.out.println("Content: " + Content);
                }
            }
        }
    }

    public static void main(String[] args) {
        String url = "jdbc:sqlserver://DESKTOP-KLF254Q;encrypt=true;trustServerCertificate=true;integratedSecurity=true;";
        ClobProcessor clobProcessor = new ClobProcessor(url);
        clobProcessor.setDbName("CLOBtest");
        clobProcessor.setTbName("Documents");

        try{
            clobProcessor.initDatabase();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            // Saving a document
//            clobProcessor.deleteDocument(1);
            clobProcessor.saveDocument(null, "This is the content of Document2 ĄĘŚĆŻŹŁÓ");
//
//            // Searching for a document
            clobProcessor.searchDocument("content");
//
//            clobProcessor.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
