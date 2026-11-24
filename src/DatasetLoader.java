import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Reads simple CSV files (header row + comma-separated values, no quoted commas)
 * and extracts numerical columns. Empty or non-numerical cells are excluded and reported.
 */
public class DatasetLoader {

    private static final int MAX_REPORTED = 50;

    public static Dataset load(File file) throws IOException {
        Dataset dataset = new Dataset();
        dataset.setFileName(file.getName());

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("The file is empty.");
            }
            if (headerLine.startsWith("\uFEFF")) {
                headerLine = headerLine.substring(1); // remove byte-order mark
            }

            String[] headers = splitLine(headerLine);
            for (int i = 0; i < headers.length; i++) {
                String name = headers[i];
                if (name.isEmpty()) {
                    name = "Column" + (i + 1);
                }
                dataset.addColumnName(name);
            }

            int columns = headers.length;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] parts = splitLine(line);
                String[] row = new String[columns];
                for (int i = 0; i < columns; i++) {
                    row[i] = (i < parts.length) ? parts[i] : "";
                }
                dataset.addRow(row);
            }
        }

        if (dataset.getRowCount() == 0) {
            throw new IOException("The file has a header but no data rows.");
        }
        return dataset;
    }

    private static String[] splitLine(String line) {
        String[] parts = line.split(",", -1);
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i].trim();
            if (p.length() >= 2 && p.startsWith("\"") && p.endsWith("\"")) {
                p = p.substring(1, p.length() - 1).trim();
            }
            parts[i] = p;
        }
        return parts;
    }

    /** Returns the number, or null if the text is empty, not a number, NaN or infinite. */
    public static Double parseValue(String text) {
        if (text == null) {
            return null;
        }
        String t = text.trim();
        if (t.isEmpty()) {
            return null;
        }
        try {
            double v = Double.parseDouble(t);
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                return null;
            }
            return v;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** A column is numerical if it has at least one valid number and valid cells are not outnumbered by invalid text. */
    public static boolean isNumericColumn(Dataset dataset, int col) {
        int valid = 0;
        int invalid = 0;
        for (int r = 0; r < dataset.getRowCount(); r++) {
            String s = dataset.getValue(r, col).trim();
            if (s.isEmpty()) {
                continue;
            }
            if (parseValue(s) != null) {
                valid++;
            } else {
                invalid++;
            }
        }
        return valid > 0 && valid >= invalid;
    }

    /**
     * Extracts valid numbers from one column.
     * excluded receives up to 50 descriptions; excludedTotal[0] counts all excluded cells.
     */
    public static double[] extractColumn(Dataset dataset, int col, List<String> excluded, long[] excludedTotal) {
        double[] buffer = new double[dataset.getRowCount()];
        int n = 0;
        for (int r = 0; r < dataset.getRowCount(); r++) {
            String raw = dataset.getValue(r, col);
            Double v = parseValue(raw);
            if (v == null) {
                excludedTotal[0]++;
                if (excluded.size() < MAX_REPORTED) {
                    excluded.add("Row " + (r + 1) + ": " + (raw.trim().isEmpty() ? "(empty)" : "'" + raw + "'"));
                }
            } else {
                buffer[n++] = v;
            }
        }
        return Arrays.copyOf(buffer, n);
    }

    /** Extracts paired values from two columns; a row is used only if BOTH cells are valid numbers. */
    public static double[][] extractPair(Dataset dataset, int colX, int colY) {
        int rows = dataset.getRowCount();
        double[] xs = new double[rows];
        double[] ys = new double[rows];
        int n = 0;
        for (int r = 0; r < rows; r++) {
            Double a = parseValue(dataset.getValue(r, colX));
            Double b = parseValue(dataset.getValue(r, colY));
            if (a != null && b != null) {
                xs[n] = a;
                ys[n] = b;
                n++;
            }
        }
        return new double[][] { Arrays.copyOf(xs, n), Arrays.copyOf(ys, n) };
    }
}