import javax.swing.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClobProcessorGUI extends JFrame{
    private String URL = "jdbc:sqlserver://DESKTOP-KLF254Q;encrypt=true;trustServerCertificate=true;integratedSecurity=true;";
    private String TB_NAME = "Documents";
    private String DB_NAME = "CLOBtest";
    private String PK_NAME = "IDtest";
    private boolean createTable = true;
    private String language = "english";
    private final Logger logger = ClobProcessor.logger;
    private ClobProcessor clobProcessor = null;
    private JFileChooser fileChooser = new JFileChooser();

    private JTabbedPane tabbedPane1;
    private JPanel panel1;
    private JTextField urlField;
    private JTextField DBnameField;
    private JTextField TBnameField;
    private JButton connectButton;
    private JButton deleteButton;
    private JButton wybierzPlikButton1;
    private JButton zapiszButton1;
    private JTextField path1;
    private JTextField IDvalue1;
    private JTextField PKnameField;
    private JTextField path2;
    private JButton wybierzPlikButton2;
    private JButton zapiszButton2;
    private JTextField deleteIDbox;
    private JTextField textContains;
    private JButton buttonContains;
    private JTextField textFreetext;
    private JButton buttonFreetext;
    private JTextArea searchOutputArea;
    private JCheckBox createTableCheckBox;
    private JComboBox languageComboBox;

    public ClobProcessorGUI() {
        languageComboBox.setModel(new DefaultComboBoxModel(new String[]{"english", "polish", "german", "french"}));
        urlField.setText(URL);
        DBnameField.setText(DB_NAME);
        TBnameField.setText(TB_NAME);
        PKnameField.setText(PK_NAME);
        connectButton.addActionListener(e -> { saveSetingsButtonPressed(); });
        wybierzPlikButton1.addActionListener(e -> { chooseFile(1); });
        wybierzPlikButton2.addActionListener(e -> { chooseFile(2); });
        zapiszButton1.addActionListener(e -> { sendCLOBtoDB(1); });
        zapiszButton2.addActionListener(e -> { sendCLOBtoDB(2); });
        deleteButton.addActionListener(e -> { deleteButtonPressed(); });
        buttonContains.addActionListener(e -> { searchDocument(1); });
        buttonFreetext.addActionListener(e -> { searchDocument(2); });
        createTableCheckBox.addActionListener(e -> {createTable = createTableCheckBox.isSelected();});
    }
    private void searchDocument(int i) {
        if(clobProcessor == null) {
            JOptionPane.showMessageDialog(null, "No connection established.");
            logger.log(Level.INFO, "No connection established.");
            return;
        }
        JTextField textBox = i == 1 ? textContains : textFreetext;
        if(textBox.getText().isEmpty()) {
            JOptionPane.showMessageDialog(null, "No text specified.");
            logger.log(Level.INFO, "No text specified for searching.");
            return;
        }
        try {
            ArrayList<String> ids = new ArrayList<>();
            if(i == 1) {
                ids = clobProcessor.searchContainDocument(textBox.getText());
                searchOutputArea.setText("");
                for(String id : ids) {
                    searchOutputArea.append(id + "\n");
                }
            }
            else if(i == 2) {
                ids = clobProcessor.searchFreeTextDocument(textBox.getText());
                searchOutputArea.setText("");
                for(String id : ids) {
                    searchOutputArea.append(id + "\n");
                }
            }
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, "Error searching document: \n" + e.getMessage());
        }
    }
    private void destroyConnection() {
        if(clobProcessor != null) {
            try {
                clobProcessor.close();
            } catch (SQLException e) {
                logger.log(Level.INFO, "Error closing connection: \n " + e.getMessage());
            }
            clobProcessor = null;
        }
    }
    private void saveSetingsButtonPressed() {
        URL = urlField.getText();
        DB_NAME = DBnameField.getText();
        TB_NAME = TBnameField.getText();
        PK_NAME = PKnameField.getText();

        language = languageComboBox.getSelectedItem().toString();
        createTable = createTableCheckBox.isSelected();

        // check if all fields are filled
        if(URL.isEmpty() || DB_NAME.isEmpty() || TB_NAME.isEmpty() || PK_NAME.isEmpty()) {
            JOptionPane.showMessageDialog(null, "Uzupełnij dane!");
            logger.log(Level.INFO, "Not all fields filled.");
            return;
        }
        logger.log(Level.INFO, "Settings saved.");
        destroyConnection();
        try {
            clobProcessor = new ClobProcessor(URL);
            clobProcessor.setDbName(DB_NAME);
            clobProcessor.setTbName(TB_NAME);
            clobProcessor.setPrimaryKeyname(PK_NAME);
            clobProcessor.setCreateTableFlag(createTable);
            clobProcessor.setLanguage(language);
            clobProcessor.initDatabase();
        } catch (SQLException e) {
            logger.log(Level.INFO, e.getMessage());
            JOptionPane.showMessageDialog(null, "Error: \n" + e.getMessage());
            return;
        }
        JOptionPane.showMessageDialog(null, "Połączono!");

    }
    private void chooseFile(int pathID) {
        fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        fileChooser.setAcceptAllFileFilterUsed(false);
        int returnVal = fileChooser.showOpenDialog(this);

        if (returnVal == JFileChooser.APPROVE_OPTION) {
            if(pathID == 1)
                path1.setText(fileChooser.getSelectedFile().getAbsolutePath());
            else if(pathID == 2)
                path2.setText(fileChooser.getSelectedFile().getAbsolutePath());
        }
    }
    public void sendCLOBtoDB(int pathID) {
        if(clobProcessor == null) {
            JOptionPane.showMessageDialog(null, "No connection established.");
            logger.log(Level.INFO, "No connection established.");
            return;
        }
        if(pathID == 1) {
            if (path1.getText().isEmpty()) {
                JOptionPane.showMessageDialog(null, "No file selected.");
                logger.log(Level.INFO, "No file selected.");
                return;
            }
            int id = 0;
            try {
                id = clobProcessor.saveDocumentFromFile(IDvalue1.getText().isEmpty() ? null : Integer.valueOf(IDvalue1.getText()), path1.getText());
                logger.log(Level.INFO, "Document saved with ID: " + id);
            } catch (SQLException e) {
                JOptionPane.showMessageDialog(null, "Error saving document: \n" + e.getMessage());
            }
        }
        else if(pathID == 2) {
            if (path2.getText().isEmpty()) {
                JOptionPane.showMessageDialog(null, "No file selected.");
                logger.log(Level.INFO, "No file selected.");
                return;
            }
            int id = 0;
            try {
                id = clobProcessor.saveDocumentFromFileByLine(path2.getText());
                logger.log(Level.INFO, "Last document saved with ID: " + id);
            } catch (SQLException e) {
                JOptionPane.showMessageDialog(null, "Error saving document: \n" + e.getMessage());
            }
        }
        JOptionPane.showMessageDialog(null, "Document saved.");
    }
    private void deleteButtonPressed() {
        if(clobProcessor == null) {
            JOptionPane.showMessageDialog(null, "No connection established.");
            logger.log(Level.INFO, "No connection established.");
            return;
        }
        if(deleteIDbox.getText().isEmpty()) {
            JOptionPane.showMessageDialog(null, "No ID specified.");
            logger.log(Level.INFO, "No ID specified for deleting.");
            return;
        }
        try {
            clobProcessor.deleteDocument(Integer.valueOf(deleteIDbox.getText()));
        } catch (SQLException e) {
            JOptionPane.showMessageDialog(null, "Error deleting document: \n" + e.getMessage());
        }
        JOptionPane.showMessageDialog(null, "Document deleted.");
    }

    public static void main(String[] args) {
        ClobProcessorGUI clobProcessorGUI = new ClobProcessorGUI();
        clobProcessorGUI.setContentPane(clobProcessorGUI.panel1);
        clobProcessorGUI.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        clobProcessorGUI.pack();
        clobProcessorGUI.setVisible(true);
    }
}
