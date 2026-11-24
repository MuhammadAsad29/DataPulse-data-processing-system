import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.border.TitledBorder;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.GradientPaint;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Insets;
import java.awt.Polygon;
import java.awt.RenderingHints;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * DataPulse: Parallel Dataset Processing System.
 * This one class contains the entire Swing GUI (cards, tables, charts, diagram)
 * and the Master side of the distributed demonstration.
 * Heavy work always runs inside a SwingWorker so the window never freezes.
 */
public class Main extends JFrame {

    // ---------------------------------------------------------------- colours
    private static final Color NAVY = new Color(0x1F2A44);
    private static final Color BLUE = new Color(0x2D7FF9);
    private static final Color TEAL = new Color(0x0E9F8E);
    private static final Color ORANGE = new Color(0xF59E0B);
    private static final Color RED = new Color(0xEF4444);
    private static final Color GREEN = new Color(0x16A34A);
    private static final Color PURPLE = new Color(0x7C3AED);
    private static final Color BG = new Color(0xF4F6FB);
    private static final Color MUTED = new Color(0x6B7280);
    private static final Color LINE = new Color(0xD1D5DB);

    // ---------------------------------------------------------------- state
    private Dataset dataset = null;
    private boolean busy = false;
    private final List<Integer> numericColumns = new ArrayList<>(); // dataset column index of each combo item
    private ParallelProcessor.Outcome lastMonitor = null;

    // ---------------------------------------------------------------- shared components
    private final JProgressBar progressBar = new JProgressBar();
    private final JLabel statusLabel = new JLabel("Ready");
    private final JComboBox<String> columnCombo = new JComboBox<>();
    private final JComboBox<String> columnCombo2 = new JComboBox<>();

    // Tab 1
    private final JLabel fileLabel = new JLabel("  No file loaded");
    private final StatCard rowsCard = new StatCard("Rows", BLUE);
    private final StatCard colsCard = new StatCard("Columns", PURPLE);
    private final StatCard numCard = new StatCard("Numerical columns", TEAL);
    private final StatCard textCard = new StatCard("Text columns", ORANGE);
    private final JTable dataTable = new JTable();
    private final DefaultTableModel columnSummaryModel =
            createModel("Column", "Type", "Valid values", "Empty / invalid");
    private final JTable columnSummaryTable = new JTable(columnSummaryModel);

    // Tab 2
    private final StatCard countCard = new StatCard("Count", BLUE);
    private final StatCard meanCard = new StatCard("Mean", TEAL);
    private final StatCard medianCard = new StatCard("Median", PURPLE);
    private final StatCard minCard = new StatCard("Minimum", ORANGE);
    private final StatCard maxCard = new StatCard("Maximum", RED);
    private final StatCard stdCard = new StatCard("Std deviation (sample)", NAVY);
    private final DefaultTableModel statsModel = createModel("Statistic", "Value", "Plain-English meaning");
    private final JTable statsTable = new JTable(statsModel);
    private final DefaultTableModel relationModel = createModel("Measure (X vs Y)", "Value");
    private final JTable relationTable = new JTable(relationModel);
    private final JTextArea excludedArea = createArea();
    private final BarChartPanel histogramChart = new BarChartPanel("Histogram of the selected column");

    // Tab 3
    private final JComboBox<String> workerCombo = createWorkerCombo();
    private final StatCard sizeCard = new StatCard("Dataset size", NAVY);
    private final StatCard seqCard = new StatCard("Sequential time", BLUE);
    private final StatCard parCard = new StatCard("Best parallel time", TEAL);
    private final StatCard speedCard = new StatCard("Best speedup", PURPLE);
    private final StatCard matchCard = new StatCard("Results match", GREEN);
    private final DefaultTableModel benchModel =
            createModel("Mode", "Workers", "Time (ms)", "Speedup", "Matches sequential");
    private final JTable benchTable = new JTable(benchModel);
    private final BarChartPanel timeChart = new BarChartPanel("Execution time");
    private final BarChartPanel speedChart = new BarChartPanel("Speedup");

    // Tab 4
    private final JComboBox<String> monitorWorkerCombo = createWorkerCombo();
    private final JComboBox<String> priorityCombo = new JComboBox<>(new String[] {
            "All NORM_PRIORITY", "Mixed MIN / NORM / MAX", "All MAX_PRIORITY" });
    private final JComboBox<String> snapshotCombo = new JComboBox<>(new String[] {
            "1 - Before start()", "2 - Right after start()", "3 - After join()" });
    private final DefaultTableModel monitorModel = createModel(
            "Worker", "ID", "Partition", "Items", "Priority", "Status", "Thread state", "Alive");
    private final JTable monitorTable = new JTable(monitorModel);
    private final BarChartPanel partitionChart = new BarChartPanel("Items per worker");
    private final BarChartPanel workerTimeChart = new BarChartPanel("Compute time per worker");

    // Tab 5
    private final JTextField port1Field = new JTextField("5001", 6);
    private final JTextField port2Field = new JTextField("5002", 6);
    private final DistributedDiagram diagram = new DistributedDiagram();
    private final DefaultTableModel distModel = createModel("Node", "Port", "Partition", "Values",
            "Data sent (bytes)", "Count", "Sum", "Mean", "Min", "Max");
    private final JTable distTable = new JTable(distModel);
    private final JLabel distSummary = new JLabel("Start the two workers, then click the button.");

    // Tab 6
    private final BarChartPanel demoChart = new BarChartPanel("Thread demonstration chart");
    private final JTextArea demoArea = createArea();

    // ================================================================ constructor

    public Main() {
        super("DataPulse - Parallel Dataset Processing System");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1200, 830);
        setLocationRelativeTo(null);
        getContentPane().setBackground(BG);

        JTabbedPane tabs = new JTabbedPane();
        tabs.setFont(new Font("SansSerif", Font.BOLD, 13));
        tabs.addTab("1  Import", buildImportTab());
        tabs.addTab("2  Statistics", buildStatsTab());
        tabs.addTab("3  Sequential vs Parallel", buildCompareTab());
        tabs.addTab("4  Worker Monitoring", buildMonitorTab());
        tabs.addTab("5  Distributed", buildDistributedTab());
        tabs.addTab("6  Thread Demos", buildDemoTab());

        JPanel selectionBar = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 6));
        selectionBar.setBackground(Color.WHITE);
        selectionBar.setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, LINE));
        selectionBar.add(label("Active column X (used by tabs 2 to 5):"));
        selectionBar.add(columnCombo);
        selectionBar.add(label("   Column Y (only for covariance / correlation):"));
        selectionBar.add(columnCombo2);

        JPanel top = new JPanel(new BorderLayout());
        top.add(new GradientHeader(), BorderLayout.NORTH);
        top.add(selectionBar, BorderLayout.SOUTH);

        progressBar.setPreferredSize(new Dimension(220, 14));
        JPanel bottom = new JPanel(new BorderLayout(10, 0));
        bottom.setBorder(BorderFactory.createEmptyBorder(5, 12, 5, 12));
        bottom.setBackground(Color.WHITE);
        bottom.add(statusLabel, BorderLayout.WEST);
        bottom.add(progressBar, BorderLayout.EAST);

        add(top, BorderLayout.NORTH);
        add(tabs, BorderLayout.CENTER);
        add(bottom, BorderLayout.SOUTH);
    }

    // ================================================================ tab builders

    private JPanel buildImportTab() {
        JPanel panel = newTabPanel();
        FancyButton chooseButton = new FancyButton("Choose CSV File...", BLUE);
        chooseButton.addActionListener(e -> chooseFile());
        fileLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
        fileLabel.setForeground(NAVY);

        JPanel north = stack(
                banner("Step 1 - Import data",
                        "Choose a CSV file. The big table is your data. The small table tells you which columns are "
                                + "numerical (usable for statistics) and how many cells are empty or invalid."),
                controls(chooseButton, fileLabel),
                cardRow(rowsCard, colsCard, numCard, textCard));

        styleTable(dataTable);
        dataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        styleTable(columnSummaryTable);

        javax.swing.JSplitPane split = new javax.swing.JSplitPane(javax.swing.JSplitPane.HORIZONTAL_SPLIT,
                titled("Dataset preview", scroll(dataTable)),
                titled("Column summary", scroll(columnSummaryTable)));
        split.setResizeWeight(0.65);
        split.setBorder(null);
        split.setOpaque(false);

        panel.add(north, BorderLayout.NORTH);
        panel.add(split, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildStatsTab() {
        JPanel panel = newTabPanel();
        FancyButton runButton = new FancyButton("Analyse Selected Column", BLUE);
        runButton.addActionListener(e -> runAnalysis(runButton));

        JPanel north = stack(
                banner("Step 2 - Statistical analysis (sequential: one thread, one pass)",
                        "Choose column X (and Y) in the bar above, then click the button. The cards show the headline "
                                + "numbers, the table explains every statistic in plain English, and the histogram shows "
                                + "how the values are spread."),
                controls(runButton),
                cardRow(countCard, meanCard, medianCard, minCard, maxCard, stdCard));

        styleTable(statsTable);
        styleTable(relationTable);
        statsTable.getColumnModel().getColumn(0).setPreferredWidth(170);
        statsTable.getColumnModel().getColumn(1).setPreferredWidth(110);
        statsTable.getColumnModel().getColumn(2).setPreferredWidth(330);

        JScrollPane relationScroll = scroll(relationTable);
        relationScroll.setPreferredSize(new Dimension(400, 150));
        JScrollPane excludedScroll = scroll(excludedArea);
        excludedScroll.setPreferredSize(new Dimension(400, 110));

        JPanel bottomLeft = new JPanel(new GridLayout(2, 1, 0, 8));
        bottomLeft.setOpaque(false);
        bottomLeft.add(titled("Covariance and correlation (X vs Y)", relationScroll));
        bottomLeft.add(titled("Excluded values in column X", excludedScroll));

        JPanel left = new JPanel(new BorderLayout(0, 8));
        left.setOpaque(false);
        left.add(titled("Statistics table", scroll(statsTable)), BorderLayout.CENTER);
        left.add(bottomLeft, BorderLayout.SOUTH);

        JPanel center = new JPanel(new GridLayout(1, 2, 10, 0));
        center.setOpaque(false);
        center.add(left);
        center.add(histogramChart);

        panel.add(north, BorderLayout.NORTH);
        panel.add(center, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildCompareTab() {
        JPanel panel = newTabPanel();
        FancyButton compareButton = new FancyButton("Compare Sequential vs Parallel", BLUE);
        compareButton.addActionListener(e -> runComparison(compareButton, false));
        FancyButton sweepButton = new FancyButton("Scalability Test (1, 2, 4, 8, N workers)", PURPLE);
        sweepButton.addActionListener(e -> runComparison(sweepButton, true));

        JPanel north = stack(
                banner("Step 3 - Sequential vs parallel (shared memory, master-worker threads)",
                        "Speedup = Sequential time / Parallel time. A GREEN bar means faster than sequential, a RED bar "
                                + "means slower: for small data the cost of creating threads is larger than the work saved. "
                                + "Each time is the best of 3 runs after one warm-up run."),
                controls(label("Worker threads:"), workerCombo, compareButton, sweepButton),
                cardRow(sizeCard, seqCard, parCard, speedCard, matchCard));

        styleTable(benchTable);
        JScrollPane tableScroll = scroll(benchTable);
        tableScroll.setPreferredSize(new Dimension(400, 175));

        JPanel charts = new JPanel(new GridLayout(1, 2, 10, 0));
        charts.setOpaque(false);
        charts.add(timeChart);
        charts.add(speedChart);

        JPanel center = new JPanel(new BorderLayout(0, 10));
        center.setOpaque(false);
        center.add(titled("Results table", tableScroll), BorderLayout.NORTH);
        center.add(charts, BorderLayout.CENTER);

        panel.add(north, BorderLayout.NORTH);
        panel.add(center, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildMonitorTab() {
        JPanel panel = newTabPanel();
        FancyButton runButton = new FancyButton("Run Monitoring Demo", PURPLE);
        runButton.addActionListener(e -> runMonitoring(runButton));
        snapshotCombo.setSelectedIndex(1);
        snapshotCombo.addActionListener(e -> fillMonitorTable());

        JPanel north = stack(
                banner("Step 4 - Worker monitoring and thread priorities",
                        "Each worker sleeps 400 ms in this demo ONLY, so its thread state can be observed "
                                + "(the timed comparison in tab 3 has no delay). Pick a snapshot to see NEW, RUNNABLE / "
                                + "TIMED_WAITING and TERMINATED. Priorities (MIN = 1, NORM = 5, MAX = 10) are scheduling "
                                + "hints only; they do not guarantee execution order or better performance."),
                controls(label("Workers:"), monitorWorkerCombo, label("Priorities:"), priorityCombo,
                        label("Show snapshot:"), snapshotCombo, runButton));

        styleTable(monitorTable);
        JScrollPane tableScroll = scroll(monitorTable);
        tableScroll.setPreferredSize(new Dimension(400, 190));

        JPanel charts = new JPanel(new GridLayout(1, 2, 10, 0));
        charts.setOpaque(false);
        charts.add(partitionChart);
        charts.add(workerTimeChart);

        JPanel center = new JPanel(new BorderLayout(0, 10));
        center.setOpaque(false);
        center.add(titled("Worker table (read with getName(), getState(), isAlive())", tableScroll), BorderLayout.NORTH);
        center.add(charts, BorderLayout.CENTER);

        panel.add(north, BorderLayout.NORTH);
        panel.add(center, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildDistributedTab() {
        JPanel panel = newTabPanel();
        FancyButton runButton = new FancyButton("Run Distributed Processing", TEAL);
        runButton.addActionListener(e -> runDistributedAction(runButton));

        JPanel north = stack(
                banner("Step 5 - Distributed memory (1 Master + 2 Worker processes, localhost sockets)",
                        "Start two workers first, each in its own PowerShell window inside the DataPulse folder:<br>"
                                + "<b>java -cp out DistributedWorker 5001</b> &nbsp;and&nbsp; "
                                + "<b>java -cp out DistributedWorker 5002</b><br>"
                                + "Then click the button. Each worker is a separate JVM with its own memory, so the numbers "
                                + "must travel through a socket."),
                controls(label("Worker 1 port:"), port1Field, label("Worker 2 port:"), port2Field, runButton));

        styleTable(distTable);
        JScrollPane tableScroll = scroll(distTable);
        tableScroll.setPreferredSize(new Dimension(400, 130));
        distSummary.setFont(new Font("SansSerif", Font.BOLD, 13));
        distSummary.setForeground(NAVY);
        distSummary.setBorder(BorderFactory.createEmptyBorder(4, 6, 4, 6));

        JPanel lower = new JPanel(new BorderLayout(0, 6));
        lower.setOpaque(false);
        lower.add(titled("Partial results and merged result", tableScroll), BorderLayout.CENTER);
        lower.add(distSummary, BorderLayout.SOUTH);

        JPanel center = new JPanel(new BorderLayout(0, 10));
        center.setOpaque(false);
        center.add(diagram, BorderLayout.CENTER);
        center.add(lower, BorderLayout.SOUTH);

        panel.add(north, BorderLayout.NORTH);
        panel.add(center, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildDemoTab() {
        JPanel panel = newTabPanel();

        FancyButton syncButton = new FancyButton("Synchronization Demo", RED);
        syncButton.addActionListener(e -> runSyncDemo(syncButton));
        FancyButton arraysButton = new FancyButton("Multiple Arrays Demo", BLUE);
        arraysButton.addActionListener(e -> runTextDemo(arraysButton, "Running multiple arrays demo...", 0));
        FancyButton ompButton = new FancyButton("OpenMP-Style Thread Demo", TEAL);
        ompButton.addActionListener(e -> runTextDemo(ompButton, "Running OpenMP-style demo...", 1));

        JPanel north = stack(
                banner("Step 6 - Thread demonstrations (no dataset needed)",
                        "Synchronization: four threads add 1 to one shared counter. Without synchronized, updates are lost "
                                + "(race condition). Multiple arrays: three threads process three different arrays. "
                                + "OpenMP-style: Java threads print messages in parallel (this is not native OpenMP)."),
                controls(syncButton, arraysButton, ompButton));

        demoChart.clear("Thread demonstration chart", "Click 'Synchronization Demo' to see the chart");
        demoArea.setText("Choose a demo above.\n");

        JPanel center = new JPanel(new GridLayout(1, 2, 10, 0));
        center.setOpaque(false);
        center.add(demoChart);
        center.add(titled("Output", scroll(demoArea)));

        panel.add(north, BorderLayout.NORTH);
        panel.add(center, BorderLayout.CENTER);
        return panel;
    }

    // ================================================================ actions: tab 1

    private void chooseFile() {
        JFileChooser chooser = new JFileChooser(new File("."));
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        runAsync(null, "Loading " + file.getName() + "...", () -> loadFile(file),
                result -> onDatasetLoaded(file, result), null);
    }

    private static class LoadResult {
        Dataset dataset;
        boolean[] numeric;
        long[][] counts; // per column: { valid values, empty or invalid values }
    }

    private static LoadResult loadFile(File file) throws IOException {
        Dataset ds = DatasetLoader.load(file);
        LoadResult r = new LoadResult();
        r.dataset = ds;
        r.numeric = new boolean[ds.getColumnCount()];
        r.counts = new long[ds.getColumnCount()][2];
        for (int c = 0; c < ds.getColumnCount(); c++) {
            r.numeric[c] = DatasetLoader.isNumericColumn(ds, c);
            for (int row = 0; row < ds.getRowCount(); row++) {
                if (DatasetLoader.parseValue(ds.getValue(row, c)) != null) {
                    r.counts[c][0]++;
                } else {
                    r.counts[c][1]++;
                }
            }
        }
        return r;
    }

    private void onDatasetLoaded(File file, LoadResult r) {
        dataset = r.dataset;
        numericColumns.clear();
        columnCombo.removeAllItems();
        columnCombo2.removeAllItems();
        columnSummaryModel.setRowCount(0);

        int numericCount = 0;
        for (int c = 0; c < dataset.getColumnCount(); c++) {
            if (r.numeric[c]) {
                numericCount++;
                numericColumns.add(c);
                columnCombo.addItem(dataset.getColumnName(c));
                columnCombo2.addItem(dataset.getColumnName(c));
            }
            columnSummaryModel.addRow(new Object[] {
                    dataset.getColumnName(c),
                    r.numeric[c] ? "Numerical" : "Text",
                    fmtInt(r.counts[c][0]),
                    fmtInt(r.counts[c][1]) });
        }
        if (columnCombo2.getItemCount() > 1) {
            columnCombo2.setSelectedIndex(1);
        }

        dataTable.setModel(new DatasetTableModel(dataset));
        for (int c = 0; c < dataTable.getColumnCount(); c++) {
            dataTable.getColumnModel().getColumn(c).setPreferredWidth(120);
        }

        fileLabel.setText("  " + file.getName());
        rowsCard.setValue(fmtInt(dataset.getRowCount()));
        colsCard.setValue(fmtInt(dataset.getColumnCount()));
        numCard.setValue(fmtInt(numericCount));
        textCard.setValue(fmtInt(dataset.getColumnCount() - numericCount));

        if (numericColumns.isEmpty()) {
            showError("The file was loaded, but it has no numerical column.");
        }
    }

    // ================================================================ actions: tab 2

    private static class AnalysisResult {
        Statistics.Result result;
        double seqMs;
        int pairCount;
        double popCov;
        double sampleCov;
        double corr;
        double[] histValues;
        String[] histLabels;
    }

    private void runAnalysis(JButton button) {
        List<String> excluded = new ArrayList<>();
        long[] excludedTotal = new long[1];
        double[] data = getSelectedData(excluded, excludedTotal);
        if (data == null) {
            return;
        }
        int colX = selectedColumn(columnCombo);
        int colY = selectedColumn(columnCombo2);
        String nameX = dataset.getColumnName(colX);
        String nameY = dataset.getColumnName(colY);

        runAsync(button, "Calculating statistics...", () -> {
            AnalysisResult a = new AnalysisResult();
            long t0 = System.nanoTime();
            Statistics.Partial p = Statistics.computeRange(data, 0, data.length); // one thread, one pass
            a.seqMs = (System.nanoTime() - t0) / 1e6;
            a.result = Statistics.Result.fromPartial(p, Statistics.median(data));

            double[][] pair = DatasetLoader.extractPair(dataset, colX, colY);
            a.pairCount = pair[0].length;
            a.popCov = Statistics.covariance(pair[0], pair[1], false);
            a.sampleCov = Statistics.covariance(pair[0], pair[1], true);
            a.corr = Statistics.correlation(pair[0], pair[1]);

            int bins = 10;
            a.histValues = new double[bins];
            a.histLabels = new String[bins];
            double min = a.result.min;
            double max = a.result.max;
            double width = (max > min) ? (max - min) / bins : 0.0;
            for (double v : data) {
                int b = (width > 0) ? (int) ((v - min) / width) : 0;
                if (b >= bins) {
                    b = bins - 1;
                }
                a.histValues[b]++;
            }
            for (int i = 0; i < bins; i++) {
                a.histLabels[i] = fmtNum(min + width * i);
            }
            return a;
        }, a -> showAnalysis(a, nameX, nameY, excluded, excludedTotal[0]), null);
    }

    private void showAnalysis(AnalysisResult a, String nameX, String nameY, List<String> excluded, long excludedTotal) {
        Statistics.Result r = a.result;
        countCard.setValue(fmtInt(r.count));
        meanCard.setValue(fmtNum(r.mean));
        medianCard.setValue(fmtNum(r.median));
        minCard.setValue(fmtNum(r.min));
        maxCard.setValue(fmtNum(r.max));
        stdCard.setValue(fmtNum(r.sampleStdDev));

        statsModel.setRowCount(0);
        addStat("Count", fmtInt(r.count), "How many valid numbers were used");
        addStat("Sum", fmtNum(r.sum), "All values added together");
        addStat("Mean", fmtNum(r.mean), "Sum divided by count (the average)");
        addStat("Median", fmtNum(r.median), "Middle value after sorting");
        addStat("Minimum", fmtNum(r.min), "Smallest value");
        addStat("Maximum", fmtNum(r.max), "Largest value");
        addStat("Range", fmtNum(r.range), "Maximum minus minimum");
        addStat("Population variance", fmtNum(r.populationVariance), "Average squared distance from the mean (divide by n)");
        addStat("Sample variance", fmtNum(r.sampleVariance), "Same, but divide by n - 1 (data is a sample)");
        addStat("Population std deviation", fmtNum(r.populationStdDev), "Square root of the population variance");
        addStat("Sample std deviation", fmtNum(r.sampleStdDev), "Square root of the sample variance");
        addStat("Time taken", String.format(Locale.US, "%.3f ms", a.seqMs), "One thread, one pass (median not included)");

        relationModel.setRowCount(0);
        relationModel.addRow(new Object[] { "Rows used (both X and Y valid)", fmtInt(a.pairCount) });
        relationModel.addRow(new Object[] { "Population covariance", fmtNum(a.popCov) });
        relationModel.addRow(new Object[] { "Sample covariance", fmtNum(a.sampleCov) });
        relationModel.addRow(new Object[] { "Pearson correlation", fmtNum(a.corr) });
        relationModel.addRow(new Object[] { "Interpretation", describeCorrelation(a.corr) });

        StringBuilder sb = new StringBuilder();
        sb.append("Excluded (empty or non-numerical) values: ").append(excludedTotal).append("\n");
        for (String s : excluded) {
            sb.append("  ").append(s).append("\n");
        }
        if (excludedTotal > excluded.size()) {
            sb.append("  ... and ").append(excludedTotal - excluded.size()).append(" more not listed\n");
        }
        excludedArea.setText(sb.toString());
        excludedArea.setCaretPosition(0);

        histogramChart.setData("Histogram of " + nameX + " (vs " + nameY + " in the table)", a.histLabels,
                a.histValues, new Color[] { BLUE }, "number of values");
    }

    private void addStat(String name, String value, String meaning) {
        statsModel.addRow(new Object[] { name, value, meaning });
    }

    private static String describeCorrelation(double r) {
        if (Double.isNaN(r)) {
            return "Undefined (needs 2+ rows and non-zero variance)";
        }
        double a = Math.abs(r);
        String strength = a >= 0.7 ? "Strong" : (a >= 0.4 ? "Moderate" : (a >= 0.1 ? "Weak" : "Very weak or none"));
        if (a < 0.1) {
            return strength;
        }
        return strength + (r > 0 ? " positive" : " negative");
    }

    // ================================================================ actions: tab 3

    private static class BenchResult {
        String[][] rows;
        String[] labels;
        double[] times;
        double[] speedups;
        boolean allMatch;
        int size;
        double seqMs;
        double bestParMs;
        double bestSpeedup;
    }

    private void runComparison(JButton button, boolean sweep) {
        List<String> excluded = new ArrayList<>();
        long[] excludedTotal = new long[1];
        double[] data = getSelectedData(excluded, excludedTotal);
        if (data == null) {
            return;
        }
        if (data.length < 2) {
            showError("At least 2 numerical values are needed.");
            return;
        }
        int[] counts;
        if (sweep) {
            TreeSet<Integer> set = new TreeSet<>();
            set.add(1);
            set.add(2);
            set.add(4);
            set.add(8);
            set.add(Runtime.getRuntime().availableProcessors());
            counts = new int[set.size()];
            int i = 0;
            for (int v : set) {
                counts[i++] = v;
            }
        } else {
            counts = new int[] { selectedWorkerCount(workerCombo) };
        }
        runAsync(button, sweep ? "Running scalability test..." : "Running comparison...",
                () -> runBenchmark(data, counts), this::showBenchmark, null);
    }

    private static BenchResult runBenchmark(double[] data, int[] workerCounts) throws Exception {
        // One untimed warm-up so the JVM's just-in-time compiler does not favour either side.
        Statistics.computeRange(data, 0, data.length);
        for (int w : workerCounts) {
            ParallelProcessor.runParallel(data, w, 0, 0, false);
        }

        long seqBest = Long.MAX_VALUE;
        Statistics.Partial seq = null;
        for (int rep = 0; rep < 3; rep++) {
            long t0 = System.nanoTime();
            Statistics.Partial p = Statistics.computeRange(data, 0, data.length);
            long ns = System.nanoTime() - t0;
            if (ns < seqBest) {
                seqBest = ns;
            }
            seq = p;
        }
        double seqMs = seqBest / 1e6;

        int n = workerCounts.length + 1;
        BenchResult r = new BenchResult();
        r.rows = new String[n][];
        r.labels = new String[n];
        r.times = new double[n];
        r.speedups = new double[n];
        r.size = data.length;
        r.seqMs = seqMs;
        r.allMatch = true;
        r.bestParMs = Double.MAX_VALUE;
        r.bestSpeedup = 0.0;

        r.rows[0] = new String[] { "Sequential", "1 (main thread)", String.format(Locale.US, "%.3f", seqMs),
                "1.00x", "reference" };
        r.labels[0] = "Sequential";
        r.times[0] = seqMs;
        r.speedups[0] = 1.0;

        for (int i = 0; i < workerCounts.length; i++) {
            long best = Long.MAX_VALUE;
            ParallelProcessor.Outcome bestOutcome = null;
            for (int rep = 0; rep < 3; rep++) {
                ParallelProcessor.Outcome o = ParallelProcessor.runParallel(data, workerCounts[i], 0, 0, false);
                if (o.nanos < best) {
                    best = o.nanos;
                    bestOutcome = o;
                }
            }
            double ms = best / 1e6;
            double speedup = (ms > 0) ? seqMs / ms : 0.0;
            boolean same = Statistics.matches(seq, bestOutcome.partial);
            int used = bestOutcome.workersUsed;

            r.rows[i + 1] = new String[] { "Parallel", String.valueOf(used), String.format(Locale.US, "%.3f", ms),
                    String.format(Locale.US, "%.2fx", speedup), same ? "YES" : "NO" };
            r.labels[i + 1] = used + (used == 1 ? " worker" : " workers");
            r.times[i + 1] = ms;
            r.speedups[i + 1] = speedup;
            if (!same) {
                r.allMatch = false;
            }
            if (ms < r.bestParMs) {
                r.bestParMs = ms;
            }
            if (speedup > r.bestSpeedup) {
                r.bestSpeedup = speedup;
            }
        }
        return r;
    }

    private void showBenchmark(BenchResult r) {
        sizeCard.setValue(fmtInt(r.size));
        seqCard.setValue(String.format(Locale.US, "%.3f ms", r.seqMs));
        parCard.setValue(String.format(Locale.US, "%.3f ms", r.bestParMs));
        speedCard.setValue(String.format(Locale.US, "%.2fx", r.bestSpeedup));
        matchCard.setValue(r.allMatch ? "YES" : "NO");

        benchModel.setRowCount(0);
        for (String[] row : r.rows) {
            benchModel.addRow(row);
        }

        Color[] timeColors = new Color[r.labels.length];
        Color[] speedColors = new Color[r.labels.length];
        for (int i = 0; i < r.labels.length; i++) {
            timeColors[i] = (i == 0) ? NAVY : BLUE;
            speedColors[i] = (i == 0) ? NAVY : (r.speedups[i] >= 1.0 ? GREEN : RED);
        }
        timeChart.setData("Execution time (best of 3)", r.labels, r.times, timeColors, "milliseconds");
        speedChart.setData("Speedup = Sequential / Parallel", r.labels, r.speedups, speedColors, "times faster");
    }

    // ================================================================ actions: tab 4

    private void runMonitoring(JButton button) {
        List<String> excluded = new ArrayList<>();
        long[] excludedTotal = new long[1];
        double[] data = getSelectedData(excluded, excludedTotal);
        if (data == null) {
            return;
        }
        int workers = selectedWorkerCount(monitorWorkerCombo);
        int mode = priorityCombo.getSelectedIndex();
        runAsync(button, "Running monitoring demo...",
                () -> ParallelProcessor.runParallel(data, workers, mode, 400, true),
                this::showMonitoring, null);
    }

    private void showMonitoring(ParallelProcessor.Outcome o) {
        lastMonitor = o;
        fillMonitorTable();

        int n = o.workers.size();
        String[] labels = new String[n];
        double[] sizes = new double[n];
        double[] times = new double[n];
        for (int i = 0; i < n; i++) {
            ParallelProcessor.StatWorker w = o.workers.get(i);
            labels[i] = "Worker-" + w.getWorkerId();
            sizes[i] = w.getTo() - w.getFrom();
            times[i] = w.getElapsedNanos() / 1e6;
        }
        partitionChart.setData("Items per worker (domain decomposition)", labels, sizes,
                new Color[] { TEAL }, "number of values");
        workerTimeChart.setData("Compute time per worker (sleep excluded)", labels, times,
                new Color[] { PURPLE }, "milliseconds");
    }

    private void fillMonitorTable() {
        if (lastMonitor == null) {
            return;
        }
        int index = snapshotCombo.getSelectedIndex();
        List<String[]> rows = (index == 0) ? lastMonitor.snapshot1
                : (index == 1 ? lastMonitor.snapshot2 : lastMonitor.snapshot3);
        monitorModel.setRowCount(0);
        for (String[] row : rows) {
            monitorModel.addRow(row);
        }
    }

    // ================================================================ actions: tab 5

    private static class DistResult {
        String[] status = new String[2];
        String[] info = new String[2];
        String[][] rows;
        String summary = "";
        boolean failed = false;
        String errorMessage = "";
    }

    private void runDistributedAction(JButton button) {
        List<String> excluded = new ArrayList<>();
        long[] excludedTotal = new long[1];
        double[] data = getSelectedData(excluded, excludedTotal);
        if (data == null) {
            return;
        }
        if (data.length < 2) {
            showError("At least 2 numerical values are needed.");
            return;
        }
        int p1;
        int p2;
        try {
            p1 = Integer.parseInt(port1Field.getText().trim());
            p2 = Integer.parseInt(port2Field.getText().trim());
        } catch (NumberFormatException ex) {
            showError("Ports must be whole numbers.");
            return;
        }
        if (p1 == p2 || p1 < 1024 || p2 < 1024 || p1 > 65535 || p2 > 65535) {
            showError("Use two different ports between 1024 and 65535.");
            return;
        }
        final int port1 = p1;
        final int port2 = p2;

        if (!busy) {
            int mid = data.length / 2;
            diagram.update("Sending...", fmtInt(mid) + " values", "Sending...", fmtInt(data.length - mid) + " values");
        }
        runAsync(button, "Contacting workers...", () -> runDistributed(data, port1, port2),
                this::showDistributed, null);
    }

    private void showDistributed(DistResult r) {
        diagram.update(r.status[0], r.info[0], r.status[1], r.info[1]);
        distModel.setRowCount(0);
        if (r.failed) {
            distSummary.setForeground(RED);
            distSummary.setText("Failed - see the red worker box. Are both workers running?");
            showError(r.errorMessage);
            return;
        }
        for (String[] row : r.rows) {
            distModel.addRow(row);
        }
        distSummary.setForeground(NAVY);
        distSummary.setText(r.summary);
    }

    // ---- Master side of distributed memory

    private static DistResult runDistributed(double[] data, int port1, int port2) throws Exception {
        int n = data.length;
        int mid = n / 2;
        int[] ports = { port1, port2 };
        int[] from = { 0, mid };
        int[] to = { mid, n };

        Statistics.Partial[] results = new Statistics.Partial[2];
        String[] errors = new String[2];
        Thread[] links = new Thread[2];

        long t0 = System.nanoTime();
        for (int i = 0; i < 2; i++) {
            final int idx = i;
            links[i] = new Thread(() -> {
                try {
                    results[idx] = requestPartial(ports[idx], data, from[idx], to[idx]);
                } catch (Exception ex) {
                    errors[idx] = (ex.getMessage() == null) ? ex.toString() : ex.getMessage();
                }
            }, "Master-Link-" + (i + 1));
            links[i].start(); // both workers receive their partition at the same time
        }
        for (Thread t : links) {
            t.join();
        }
        double ms = (System.nanoTime() - t0) / 1e6;

        DistResult r = new DistResult();
        StringBuilder errorText = new StringBuilder();
        for (int i = 0; i < 2; i++) {
            if (errors[i] == null) {
                r.status[i] = "Done";
                r.info[i] = fmtInt(to[i] - from[i]) + " values";
            } else {
                r.status[i] = "Error";
                r.info[i] = (errors[i].length() > 34) ? errors[i].substring(0, 34) + "..." : errors[i];
                r.failed = true;
                errorText.append("Worker ").append(i + 1).append(" (port ").append(ports[i]).append("): ")
                        .append(errors[i]).append("\n");
            }
        }
        if (r.failed) {
            r.errorMessage = errorText.toString();
            return r;
        }

        Statistics.Partial combined = Statistics.Partial.merge(results[0], results[1]);
        Statistics.Partial sequential = Statistics.computeRange(data, 0, n);
        boolean same = Statistics.matches(sequential, combined);
        Statistics.Result fin = Statistics.Result.fromPartial(combined, Statistics.median(data));

        r.rows = new String[3][];
        for (int i = 0; i < 2; i++) {
            Statistics.Partial p = results[i];
            r.rows[i] = new String[] { "Worker " + (i + 1), String.valueOf(ports[i]),
                    "[" + from[i] + ", " + to[i] + ")", fmtInt(to[i] - from[i]),
                    fmtInt((long) (to[i] - from[i]) * 8L), fmtInt(p.count), fmtNum(p.sum), fmtNum(p.mean),
                    fmtNum(p.min), fmtNum(p.max) };
        }
        r.rows[2] = new String[] { "Master (merged)", "-", "[0, " + n + ")", fmtInt(n), fmtInt((long) n * 8L),
                fmtInt(combined.count), fmtNum(combined.sum), fmtNum(combined.mean), fmtNum(combined.min),
                fmtNum(combined.max) };

        r.summary = String.format(Locale.US,
                "Round-trip time: %.3f ms   |   Matches sequential: %s   |   Median: %s   |   Sample std dev: %s",
                ms, same ? "YES" : "NO", fmtNum(fin.median), fmtNum(fin.sampleStdDev));
        return r;
    }

    private static Statistics.Partial requestPartial(int port, double[] data, int from, int to) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 3000);
            socket.setSoTimeout(30000);
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            out.writeInt(to - from);
            for (int i = from; i < to; i++) {
                out.writeDouble(data[i]);
            }
            out.flush();

            Statistics.Partial p = new Statistics.Partial();
            p.count = in.readLong();
            p.sum = in.readDouble();
            p.mean = in.readDouble();
            p.m2 = in.readDouble();
            p.min = in.readDouble();
            p.max = in.readDouble();
            return p;
        } catch (ConnectException | SocketTimeoutException ex) {
            throw new IOException("could not connect or timed out. Is DistributedWorker running on port " + port + "?");
        }
    }

    // ================================================================ actions: tab 6

    private void runSyncDemo(JButton button) {
        runAsync(button, "Running synchronization demo...", () -> ParallelProcessor.synchronizationCounts(4, 100000),
                counts -> {
                    long expected = counts[0];
                    demoChart.setData("Shared counter: 4 threads x 100,000 increments",
                            new String[] { "Expected", "Without synchronized", "With synchronized" },
                            new double[] { counts[0], counts[1], counts[2] },
                            new Color[] { NAVY, RED, GREEN }, "final counter value");
                    demoArea.setText("SYNCHRONIZATION DEMONSTRATION\n\n"
                            + String.format(Locale.US, "Expected final value:           %,d%n", expected)
                            + String.format(Locale.US, "Without synchronization:       %,d  (lost updates: %,d)%n",
                                    counts[1], expected - counts[1])
                            + String.format(Locale.US, "With synchronized:             %,d  (lost updates: %,d)%n%n",
                                    counts[2], expected - counts[2])
                            + "Why: counter++ is three steps (read, add, write). Two threads can read the same value\n"
                            + "and both write back the same result, so one update is lost (race condition).\n"
                            + "A synchronized method lets only one thread execute it at a time, so no update is lost.\n"
                            + "The race is timing dependent; click the button several times.\n");
                    demoArea.setCaretPosition(0);
                }, null);
    }

    private void runTextDemo(JButton button, String message, int which) {
        runAsync(button, message, () -> (which == 0)
                        ? ParallelProcessor.multipleArraysDemo(1000000)
                        : ParallelProcessor.openMpStyleDemo(4),
                text -> {
                    demoChart.clear("Thread demonstration chart", "No chart for this demo - see the output");
                    demoArea.setText(text);
                    demoArea.setCaretPosition(0);
                }, null);
    }

    // ================================================================ helpers: data and tasks

    /** Reads the selected column X on the GUI thread; shows an error and returns null if impossible. */
    private double[] getSelectedData(List<String> excluded, long[] excludedTotal) {
        if (dataset == null || numericColumns.isEmpty()) {
            showError("Import a CSV file with at least one numerical column first (tab 1).");
            return null;
        }
        int col = selectedColumn(columnCombo);
        double[] data = DatasetLoader.extractColumn(dataset, col, excluded, excludedTotal);
        if (data.length == 0) {
            showError("The selected column has no valid numerical values.");
            return null;
        }
        return data;
    }

    private int selectedColumn(JComboBox<String> combo) {
        return numericColumns.get(combo.getSelectedIndex());
    }

    private static JComboBox<String> createWorkerCombo() {
        return new JComboBox<>(new String[] { "2 workers", "4 workers",
                "Logical processors (" + Runtime.getRuntime().availableProcessors() + ")" });
    }

    private static int selectedWorkerCount(JComboBox<String> combo) {
        int index = combo.getSelectedIndex();
        if (index == 0) {
            return 2;
        }
        if (index == 1) {
            return 4;
        }
        return Runtime.getRuntime().availableProcessors();
    }

    /** Runs a task in the background (SwingWorker) and hands its result to onSuccess on the GUI thread. */
    private <T> void runAsync(JButton button, String message, Callable<T> task,
                              Consumer<T> onSuccess, Consumer<String> onError) {
        if (busy) {
            showError("Please wait: another task is still running.");
            return;
        }
        busy = true;
        if (button != null) {
            button.setEnabled(false);
        }
        setBusy(true, message);

        new SwingWorker<T, Void>() {
            @Override
            protected T doInBackground() throws Exception {
                return task.call();
            }

            @Override
            protected void done() {
                busy = false;
                if (button != null) {
                    button.setEnabled(true);
                }
                setBusy(false, "Ready");
                try {
                    onSuccess.accept(get());
                } catch (Exception ex) {
                    String m = rootMessage(ex);
                    if (onError != null) {
                        onError.accept(m);
                    }
                    showError(m);
                }
            }
        }.execute();
    }

    private void setBusy(boolean isBusy, String message) {
        progressBar.setIndeterminate(isBusy);
        statusLabel.setText(message);
    }

    private void showError(String message) {
        JOptionPane.showMessageDialog(this, message, "DataPulse", JOptionPane.ERROR_MESSAGE);
    }

    private static String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String m = current.getMessage();
        return (m == null) ? current.toString() : m;
    }

    // ================================================================ helpers: formatting

    private static String fmtInt(long v) {
        return String.format(Locale.US, "%,d", v);
    }

    private static String fmtNum(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        if (Double.isInfinite(v)) {
            return "Infinity";
        }
        double a = Math.abs(v);
        if (a >= 1000) {
            return String.format(Locale.US, "%,.2f", v);
        }
        if (a >= 100) {
            return String.format(Locale.US, "%.2f", v);
        }
        if (a >= 1) {
            return String.format(Locale.US, "%.3f", v);
        }
        if (a == 0) {
            return "0";
        }
        return String.format(Locale.US, "%.4f", v);
    }

    // ================================================================ helpers: GUI building

    private static JPanel newTabPanel() {
        JPanel p = new JPanel(new BorderLayout(10, 10));
        p.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));
        p.setBackground(BG);
        return p;
    }

    private static JPanel stack(java.awt.Component... parts) {
        JPanel p = new JPanel(new GridBagLayout());
        p.setOpaque(false);
        GridBagConstraints gc = new GridBagConstraints();
        gc.gridx = 0;
        gc.weightx = 1.0;
        gc.fill = GridBagConstraints.HORIZONTAL;
        gc.insets = new Insets(0, 0, 8, 0);
        for (int i = 0; i < parts.length; i++) {
            gc.gridy = i;
            p.add(parts[i], gc);
        }
        return p;
    }

    private static JPanel controls(java.awt.Component... items) {
        JPanel p = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 2));
        p.setOpaque(false);
        for (java.awt.Component c : items) {
            p.add(c);
        }
        return p;
    }

    private static JPanel cardRow(StatCard... cards) {
        JPanel p = new JPanel(new GridLayout(1, cards.length, 10, 0));
        p.setOpaque(false);
        for (StatCard c : cards) {
            p.add(c);
        }
        return p;
    }

    private static JLabel label(String text) {
        JLabel l = new JLabel(text);
        l.setFont(new Font("SansSerif", Font.BOLD, 13));
        l.setForeground(NAVY);
        return l;
    }

    private static JLabel banner(String title, String body) {
        JLabel l = new JLabel("<html><body style='width:900px'><b>" + title + "</b><br>" + body + "</body></html>");
        l.setOpaque(true);
        l.setBackground(new Color(0xE8F1FF));
        l.setForeground(NAVY);
        l.setFont(new Font("SansSerif", Font.PLAIN, 13));
        l.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 5, 0, 0, BLUE),
                BorderFactory.createEmptyBorder(8, 12, 8, 12)));
        return l;
    }

    private static JPanel titled(String title, java.awt.Component content) {
        JPanel p = new JPanel(new BorderLayout());
        p.setOpaque(false);
        TitledBorder tb = BorderFactory.createTitledBorder(BorderFactory.createLineBorder(LINE), " " + title + " ");
        tb.setTitleFont(new Font("SansSerif", Font.BOLD, 13));
        tb.setTitleColor(NAVY);
        p.setBorder(tb);
        p.add(content, BorderLayout.CENTER);
        return p;
    }

    private static JScrollPane scroll(java.awt.Component c) {
        JScrollPane sp = new JScrollPane(c);
        sp.setBorder(BorderFactory.createEmptyBorder());
        sp.getViewport().setBackground(Color.WHITE);
        return sp;
    }

    private static JTextArea createArea() {
        JTextArea area = new JTextArea();
        area.setEditable(false);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        area.setMargin(new Insets(6, 8, 6, 8));
        return area;
    }

    private static DefaultTableModel createModel(String... columns) {
        return new DefaultTableModel(columns, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
    }

    private static void styleTable(JTable table) {
        table.setRowHeight(26);
        table.setShowGrid(false);
        table.setIntercellSpacing(new Dimension(0, 0));
        table.setFont(new Font("SansSerif", Font.PLAIN, 13));
        table.setFillsViewportHeight(true);
        table.setDefaultRenderer(Object.class, new ZebraRenderer());
        JTableHeader header = table.getTableHeader();
        header.setDefaultRenderer(new HeaderRenderer());
        header.setReorderingAllowed(false);
        header.setPreferredSize(new Dimension(header.getPreferredSize().width, 30));
    }

    private static Color textColorFor(String text) {
        switch (text) {
            case "RUNNABLE":
            case "YES":
            case "Alive":
            case "Done":
                return GREEN;
            case "TIMED_WAITING":
                return new Color(0xB45309);
            case "WAITING":
            case "BLOCKED":
            case "NO":
                return RED;
            case "TERMINATED":
            case "Not alive":
                return MUTED;
            case "NEW":
                return BLUE;
            default:
                return null;
        }
    }

    // ================================================================ custom GUI classes

    /** Table header: dark blue background, white bold text. */
    private static class HeaderRenderer extends DefaultTableCellRenderer {
        HeaderRenderer() {
            setHorizontalAlignment(CENTER);
            setOpaque(true);
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
                                                       boolean hasFocus, int row, int column) {
            super.getTableCellRendererComponent(table, value, false, false, row, column);
            setBackground(NAVY);
            setForeground(Color.WHITE);
            setFont(new Font("SansSerif", Font.BOLD, 13));
            setBorder(BorderFactory.createEmptyBorder(0, 8, 0, 8));
            return this;
        }
    }

    /** Table rows: alternating colours; thread states and YES / NO are coloured. */
    private static class ZebraRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
                                                       boolean hasFocus, int row, int column) {
            super.getTableCellRendererComponent(table, value, isSelected, false, row, column);
            String text = (value == null) ? "" : value.toString();
            setBorder(BorderFactory.createEmptyBorder(0, 8, 0, 8));
            setHorizontalAlignment((column == 0 || text.length() > 28) ? LEFT : CENTER);
            setFont(table.getFont());
            if (isSelected) {
                setBackground(new Color(0xBFDBFE));
                setForeground(NAVY);
            } else {
                setBackground(row % 2 == 0 ? Color.WHITE : new Color(0xF3F6FD));
                setForeground(new Color(0x1F2937));
            }
            Color special = textColorFor(text);
            if (special != null && !isSelected) {
                setForeground(special);
                setFont(table.getFont().deriveFont(Font.BOLD));
            }
            return this;
        }
    }

    /** Rounded coloured button that looks the same in every look and feel. */
    private static class FancyButton extends JButton {
        private final Color base;
        private boolean hover = false;

        FancyButton(String text, Color base) {
            super(text);
            this.base = base;
            setForeground(Color.WHITE);
            setFont(new Font("SansSerif", Font.BOLD, 13));
            setContentAreaFilled(false);
            setBorderPainted(false);
            setFocusPainted(false);
            setOpaque(false);
            setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            setBorder(BorderFactory.createEmptyBorder(8, 18, 8, 18));
            addMouseListener(new MouseAdapter() {
                @Override
                public void mouseEntered(MouseEvent e) {
                    hover = true;
                    repaint();
                }

                @Override
                public void mouseExited(MouseEvent e) {
                    hover = false;
                    repaint();
                }
            });
        }

        @Override
        protected void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            Color c = !isEnabled() ? new Color(0x9CA3AF) : (hover ? base.darker() : base);
            g2.setColor(c);
            g2.fillRoundRect(0, 0, getWidth(), getHeight(), 14, 14);
            g2.dispose();
            super.paintComponent(g);
        }
    }

    /** Small white card with a coloured left edge, a title and one big value. */
    private static class StatCard extends JPanel {
        private final JLabel valueLabel = new JLabel("-");

        StatCard(String title, Color accent) {
            setLayout(new BorderLayout(0, 2));
            setBackground(Color.WHITE);
            setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(new Color(0xE5E7EB)),
                    BorderFactory.createCompoundBorder(
                            BorderFactory.createMatteBorder(0, 6, 0, 0, accent),
                            BorderFactory.createEmptyBorder(7, 10, 7, 10))));
            JLabel titleLabel = new JLabel(title);
            titleLabel.setFont(new Font("SansSerif", Font.PLAIN, 12));
            titleLabel.setForeground(MUTED);
            valueLabel.setFont(new Font("SansSerif", Font.BOLD, 19));
            valueLabel.setForeground(accent);
            add(titleLabel, BorderLayout.NORTH);
            add(valueLabel, BorderLayout.CENTER);
            setPreferredSize(new Dimension(150, 62));
        }

        void setValue(String text) {
            valueLabel.setText(text);
        }
    }

    /** Dark-to-blue banner at the top of the window. */
    private static class GradientHeader extends JPanel {
        GradientHeader() {
            setLayout(new BorderLayout());
            setOpaque(false);
            setBorder(BorderFactory.createEmptyBorder(14, 22, 14, 22));
            JLabel title = new JLabel("DataPulse");
            title.setFont(new Font("SansSerif", Font.BOLD, 28));
            title.setForeground(Color.WHITE);
            JLabel subtitle = new JLabel("Parallel Dataset Processing System   |   Sequential - Shared-memory threads - Distributed sockets");
            subtitle.setFont(new Font("SansSerif", Font.PLAIN, 13));
            subtitle.setForeground(new Color(0xDBEAFE));
            JPanel text = new JPanel(new GridLayout(2, 1));
            text.setOpaque(false);
            text.add(title);
            text.add(subtitle);
            add(text, BorderLayout.WEST);
        }

        @Override
        protected void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setPaint(new GradientPaint(0, 0, NAVY, getWidth(), 0, BLUE));
            g2.fillRect(0, 0, getWidth(), getHeight());
            g2.dispose();
            super.paintComponent(g);
        }
    }

    /** Simple vertical bar chart drawn with Graphics2D (no external library). */
    private static class BarChartPanel extends JPanel {
        private String title;
        private String[] labels = new String[0];
        private double[] values = new double[0];
        private Color[] colors = new Color[] { BLUE };
        private String unit = "";
        private String emptyMessage = "Run the analysis to see this chart";

        BarChartPanel(String title) {
            this.title = title;
            setBackground(Color.WHITE);
            setPreferredSize(new Dimension(420, 250));
            setBorder(BorderFactory.createLineBorder(new Color(0xE5E7EB)));
        }

        void setData(String title, String[] labels, double[] values, Color[] colors, String unit) {
            this.title = title;
            this.labels = labels;
            this.values = values;
            this.colors = (colors == null || colors.length == 0) ? new Color[] { BLUE } : colors;
            this.unit = unit;
            repaint();
        }

        void clear(String title, String message) {
            this.title = title;
            this.values = new double[0];
            this.labels = new String[0];
            this.emptyMessage = message;
            repaint();
        }

        private static double niceCeil(double max) {
            double exp = Math.pow(10, Math.floor(Math.log10(max)));
            double f = max / exp;
            double nf = (f <= 1) ? 1 : (f <= 2) ? 2 : (f <= 5) ? 5 : 10;
            return nf * exp;
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
            int w = getWidth();
            int h = getHeight();

            g2.setFont(new Font("SansSerif", Font.BOLD, 14));
            g2.setColor(NAVY);
            g2.drawString(title, 14, 22);
            g2.setFont(new Font("SansSerif", Font.PLAIN, 11));
            FontMetrics fm = g2.getFontMetrics();
            if (!unit.isEmpty() && values.length > 0) {
                g2.setColor(MUTED);
                g2.drawString("unit: " + unit, 14, 36);
            }

            if (values.length == 0) {
                g2.setColor(MUTED);
                g2.setFont(new Font("SansSerif", Font.ITALIC, 13));
                FontMetrics fm2 = g2.getFontMetrics();
                g2.drawString(emptyMessage, Math.max(10, (w - fm2.stringWidth(emptyMessage)) / 2), h / 2);
                g2.dispose();
                return;
            }

            int left = 58;
            int right = 16;
            int top = 46;
            int bottom = 40;
            int plotW = w - left - right;
            int plotH = h - top - bottom;
            if (plotW <= 10 || plotH <= 10) {
                g2.dispose();
                return;
            }

            double max = 0;
            for (double v : values) {
                if (!Double.isNaN(v) && !Double.isInfinite(v)) {
                    max = Math.max(max, v);
                }
            }
            if (max <= 0) {
                max = 1;
            }
            double niceMax = niceCeil(max);

            for (int i = 0; i <= 4; i++) {
                int y = top + plotH - (int) (plotH * i / 4.0);
                g2.setColor(new Color(0xE5E7EB));
                g2.drawLine(left, y, left + plotW, y);
                String s = fmtNum(niceMax * i / 4.0);
                g2.setColor(MUTED);
                g2.drawString(s, left - 6 - fm.stringWidth(s), y + 4);
            }

            int n = values.length;
            double slot = plotW / (double) n;
            int barW = (int) Math.max(4, Math.min(70, slot * 0.62));

            int maxLabelW = 0;
            for (String s : labels) {
                maxLabelW = Math.max(maxLabelW, fm.stringWidth(s));
            }
            int step = (maxLabelW + 6 > slot) ? (int) Math.ceil((maxLabelW + 6) / slot) : 1;

            for (int i = 0; i < n; i++) {
                double v = (Double.isNaN(values[i]) || Double.isInfinite(values[i])) ? 0 : values[i];
                int barH = (int) Math.round(plotH * v / niceMax);
                int x = left + (int) (slot * i + (slot - barW) / 2.0);
                int y = top + plotH - barH;

                g2.setColor(colors[i % colors.length]);
                g2.fillRoundRect(x, y, barW, Math.max(barH, 2), 6, 6);

                String vs = fmtNum(v);
                g2.setColor(NAVY);
                g2.drawString(vs, x + barW / 2 - fm.stringWidth(vs) / 2, y - 4);

                if (i % step == 0 && i < labels.length) {
                    g2.setColor(new Color(0x374151));
                    g2.drawString(labels[i], x + barW / 2 - fm.stringWidth(labels[i]) / 2, top + plotH + 16);
                }
            }
            g2.setColor(new Color(0x9CA3AF));
            g2.drawLine(left, top + plotH, left + plotW, top + plotH);
            g2.dispose();
        }
    }

    /** Picture of the Master and the two Worker processes with their current status. */
    private static class DistributedDiagram extends JPanel {
        private final String[] status = { "Idle", "Idle" };
        private final String[] info = { "", "" };

        DistributedDiagram() {
            setBackground(Color.WHITE);
            setPreferredSize(new Dimension(700, 230));
            setBorder(BorderFactory.createLineBorder(new Color(0xE5E7EB)));
        }

        void update(String status1, String info1, String status2, String info2) {
            status[0] = status1;
            info[0] = info1;
            status[1] = status2;
            info[1] = info2;
            repaint();
        }

        private static Color statusColor(String s) {
            switch (s) {
                case "Sending...":
                    return ORANGE;
                case "Done":
                    return GREEN;
                case "Error":
                    return RED;
                default:
                    return new Color(0x9CA3AF);
            }
        }

        private void drawBox(Graphics2D g2, int x, int y, int w, int h, Color color, String l1, String l2, String l3) {
            g2.setColor(color);
            g2.fillRoundRect(x, y, w, h, 18, 18);
            g2.setColor(Color.WHITE);
            g2.setFont(new Font("SansSerif", Font.BOLD, 14));
            g2.drawString(l1, x + 14, y + 24);
            g2.setFont(new Font("SansSerif", Font.PLAIN, 12));
            g2.drawString(l2, x + 14, y + 44);
            g2.drawString(l3, x + 14, y + 62);
        }

        private void drawArrow(Graphics2D g2, int x1, int y1, int x2, int y2, Color color, boolean dashed) {
            g2.setColor(color);
            if (dashed) {
                g2.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10f,
                        new float[] { 7f, 5f }, 0f));
            } else {
                g2.setStroke(new BasicStroke(2f));
            }
            g2.drawLine(x1, y1, x2, y2);
            g2.setStroke(new BasicStroke(1f));
            double angle = Math.atan2(y2 - y1, x2 - x1);
            int len = 10;
            Polygon head = new Polygon();
            head.addPoint(x2, y2);
            head.addPoint((int) (x2 - len * Math.cos(angle - Math.PI / 7)), (int) (y2 - len * Math.sin(angle - Math.PI / 7)));
            head.addPoint((int) (x2 - len * Math.cos(angle + Math.PI / 7)), (int) (y2 - len * Math.sin(angle + Math.PI / 7)));
            g2.fillPolygon(head);
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
            int w = getWidth();
            int h = getHeight();

            g2.setFont(new Font("SansSerif", Font.BOLD, 14));
            g2.setColor(NAVY);
            g2.drawString("Distributed-memory processing (separate processes, data travels through sockets)", 14, 22);

            int boxH = 74;
            int masterX = 30;
            int masterW = 210;
            int masterY = h / 2 - boxH / 2 + 8;
            int workerW = 230;
            int workerX = w - workerW - 30;
            int workerY1 = 38;
            int workerY2 = h - boxH - 14;

            drawBox(g2, masterX, masterY, masterW, boxH, NAVY, "MASTER (this application)",
                    "1. split array in 2 parts", "4. merge partial results");
            for (int i = 0; i < 2; i++) {
                int y = (i == 0) ? workerY1 : workerY2;
                drawBox(g2, workerX, y, workerW, boxH, statusColor(status[i]),
                        "WORKER " + (i + 1) + " (separate JVM)", "status: " + status[i],
                        info[i].isEmpty() ? "waiting for a partition" : info[i]);

                int wy = y + boxH / 2;
                int my = masterY + boxH / 2;
                int mx = masterX + masterW;
                // send arrow (solid) and result arrow (dashed)
                drawArrow(g2, mx + 4, my + (i == 0 ? -10 : 10), workerX - 4, wy - 10, BLUE, false);
                drawArrow(g2, workerX - 4, wy + 10, mx + 4, my + (i == 0 ? -2 : 18), TEAL, true);
                g2.setFont(new Font("SansSerif", Font.PLAIN, 11));
                int labelX = (mx + workerX) / 2 - 60;
                int labelY = (my + wy) / 2 + (i == 0 ? -16 : 28);
                g2.setColor(BLUE);
                g2.drawString("2. send partition", labelX, labelY);
                g2.setColor(TEAL);
                g2.drawString("3. return partial result", labelX, labelY + 13);
            }
            g2.dispose();
        }
    }

    /** Lets the JTable show the dataset directly, without copying it. */
    private static class DatasetTableModel extends AbstractTableModel {
        private final Dataset data;

        DatasetTableModel(Dataset data) {
            this.data = data;
        }

        @Override
        public int getRowCount() {
            return data.getRowCount();
        }

        @Override
        public int getColumnCount() {
            return data.getColumnCount();
        }

        @Override
        public String getColumnName(int column) {
            return data.getColumnName(column);
        }

        @Override
        public Object getValueAt(int row, int column) {
            return data.getValue(row, column);
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                    if ("Nimbus".equals(info.getName())) {
                        UIManager.setLookAndFeel(info.getClassName());
                        break;
                    }
                }
            } catch (Exception ignored) {
                // fall back to the default look and feel
            }
            new Main().setVisible(true);
        });
    }
}