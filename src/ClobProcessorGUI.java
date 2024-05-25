import javax.swing.*;

public class ClobProcessorGUI {
    private String DB_URL = "jdbc:sqlserver://DESKTOP-KLF254Q;encrypt=true;trustServerCertificate=true;integratedSecurity=true;";
    private JPanel panel1;
    private JTabbedPane tasbbedPane1;
    private JTextField textField1;

    public ClobProcessorGUI() {
        textField1.setText(DB_URL);
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("ClobProcessorGUI");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
