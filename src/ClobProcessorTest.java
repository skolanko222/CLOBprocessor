import org.junit.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;


public class ClobProcessorTest {

    static String DB_URL = "jdbc:sqlserver://DESKTOP-KLF254Q;encrypt=true;trustServerCertificate=true;integratedSecurity=true;";
    static String DB_NAME = "CLOBtest1";
    static String TB_NAME = "CLOBtest1";
    static String PK_NAME = "IDtest";

    private static ClobProcessor clobProcessor;

    @BeforeClass
    public static void setUpClass() throws SQLException {
        clobProcessor = new ClobProcessor(DB_URL);
        clobProcessor.setDbName(DB_NAME);
        clobProcessor.setTbName(TB_NAME);
        clobProcessor.setPrimaryKeyname(PK_NAME);
        clobProcessor.setCreateTableFlag(true);
        clobProcessor.setLanguage("english");
    }

    @Before
    public void setUp() throws SQLException {
        try {
            clobProcessor.dropDatabase();
        } catch (SQLException ignored) {}
        clobProcessor.initDatabase();
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        // drop database
        clobProcessor.dropDatabase();
        clobProcessor.close();
    }

    @Test
    public void testSaveDocument() throws SQLException {
        Integer id = 1;
        if(clobProcessor.checkIfDocumentExists(id))
            clobProcessor.deleteDocument(id);

        String content = "This is a test document.";
        clobProcessor.saveDocument(id, content);
        System.out.println(clobProcessor.checkIfDocumentExists(id));
        assertTrue(clobProcessor.checkIfDocumentExists(id));
        clobProcessor.deleteDocument(id);

    }
    @Test
    public void testSaveDocumentNoID() throws SQLException {
        String content = "This is a very unique document.";
        int id = clobProcessor.saveDocument(null, content);
        System.out.println(id);
        assertTrue(clobProcessor.checkIfDocumentExists(id));
    }

    @Test
    public void testDeleteDocument() throws SQLException {
        Integer id = 1;
        String content = "This document will be deleted.";

        if(!clobProcessor.checkIfDocumentExists(id))
            clobProcessor.saveDocument(id, content);

        clobProcessor.deleteDocument(id);
        assertFalse(clobProcessor.checkIfDocumentExists(id));
    }

    @Test
    public void testSearchContainDocument() throws SQLException, InterruptedException {
        String content = "Searchable content.";
        clobProcessor.saveDocument(null, content);
        //wait for the document to be indexed
        Thread.sleep(5000);

        ArrayList<String> results = clobProcessor.searchContainDocument("searchable");
        assertFalse(results.isEmpty());
    }

    @Test
    public void testSearchFreeTextDocument() throws SQLException, InterruptedException {
        String content = "Safe searchable content.";
        clobProcessor.saveDocument(null, content);
        //wait for the document to be indexed
        Thread.sleep(5000);
        ArrayList<String> results = clobProcessor.searchFreeTextDocument("contents");
        System.out.println(results);
        assertFalse(results.isEmpty());
    }
    @Test
    public void testSearchFreeTextDocument2() throws SQLException, InterruptedException {
        String searchTerm = "content";
        clobProcessor.saveDocument(null, "This is a test content.");
        //wait for the document to be indexed
        Thread.sleep(5000);
        ArrayList<String> results = clobProcessor.searchContainDocument(searchTerm);
        System.out.println(results);
        assertFalse(results.isEmpty());
    }
    @Test
    public void testReadTextFileAndStore() throws SQLException {
        String content = clobProcessor.readTextFile("./exampleClobData2.txt");
        int id = clobProcessor.saveDocument(null, content);
        assertTrue(clobProcessor.checkIfDocumentExists(id));
    }

    @Test
    public void testReadTextFileAndStoreLineByLine() throws SQLException {
        Integer id = clobProcessor.saveDocumentFromFileByLine("./exampleClobData2.txt");
        assertTrue(clobProcessor.checkIfDocumentExists(id));
    }
}
