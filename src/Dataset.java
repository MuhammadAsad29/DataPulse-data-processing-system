import java.util.ArrayList;
import java.util.List;

/**
 * Holds the imported CSV data exactly as text.
 * Values are converted to numbers only when a column is selected for analysis.
 */
public class Dataset {

    private final List<String> columnNames = new ArrayList<>();
    private final List<String[]> rows = new ArrayList<>();
    private String fileName = "";

    public void addColumnName(String name) {
        columnNames.add(name);
    }

    public void addRow(String[] row) {
        rows.add(row);
    }

    public int getRowCount() {
        return rows.size();
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public String getColumnName(int col) {
        return columnNames.get(col);
    }

    public String getValue(int row, int col) {
        return rows.get(row)[col];
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}