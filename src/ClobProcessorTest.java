import org.junit.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;


public class ClobProcessorTest {

    private static final String DB_URL = "jdbc:sqlserver://DESKTOP-KLF254Q;encrypt=true;trustServerCertificate=true;integratedSecurity=true;";
    private static final String DB_NAME = "CLOBtest";
    private static final String TB_NAME = "Documents2";
    private static final String PK_NAME = "IDtest";

    private static ClobProcessor clobProcessor;

    @BeforeClass
    public static void setUpClass() throws SQLException {
        clobProcessor = new ClobProcessor(DB_URL);
        clobProcessor.setDbName(DB_NAME);
        clobProcessor.setTbName(TB_NAME);
        clobProcessor.setPrimaryKeyname(PK_NAME);
    }

    @Before
    public void setUp() throws SQLException {
        clobProcessor.initDatabase();
    }

    @AfterClass
    public static void tearDown() throws SQLException {
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
    public void testSearchFreeTextDocument() throws SQLException {
        String content = "Safe searchable content.";
        clobProcessor.saveDocument(null, content);
        //wait for the document to be indexed
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ArrayList<String> results = clobProcessor.searchFreeTextDocument("contents");
        System.out.println(results);
        assertFalse(results.isEmpty());
    }
    @Test
    public void testSearchFreeTextDocument2() throws SQLException {
        String searchTerm = "content";
        ArrayList<String> results = clobProcessor.searchContainDocument(searchTerm);
        System.out.println(results);
        assertFalse(results.isEmpty());
    }
    @Test
    public void testReadTextFileAndStore() throws SQLException {
        String content = clobProcessor.readTextFile("./bible.txt");
        int id = clobProcessor.saveDocument(null, content);
        assertTrue(clobProcessor.checkIfDocumentExists(id));
    }

    @Test
    public void testReadTextFileAndStoreLineByLine() throws SQLException {
        Integer id = clobProcessor.saveDocumentFromFileByLine("./bible.txt");
        assertTrue(clobProcessor.checkIfDocumentExists(id));
    }
}
