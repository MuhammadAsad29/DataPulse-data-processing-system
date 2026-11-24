import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Shared-memory parallel processing with plain Java threads (master-worker).
 *
 * SHARED DATA   : the double[] data array. Every worker holds a reference to the SAME array (no copy).
 *                 Workers only READ it, so reading needs no lock.
 * PRIVATE DATA  : each worker's partition bounds [from, to), its own Partial result and its own timing.
 * COMBINATION   : the master thread waits (join) for all workers, then merges the partial results.
 */
public class ParallelProcessor {

    /** One worker thread = one partition. */
    public static class StatWorker extends Thread {
        private final int workerId;
        private final double[] sharedData;
        private final int from;
        private final int to;
        private final long sleepMillis;
        private final List<StatWorker> sharedFinishedList;
        private volatile String status = "CREATED";
        private Statistics.Partial partial;
        private long elapsedNanos = 0;

        public StatWorker(int workerId, double[] sharedData, int from, int to,
                          long sleepMillis, List<StatWorker> sharedFinishedList) {
            super("Worker-" + workerId);
            this.workerId = workerId;
            this.sharedData = sharedData;
            this.from = from;
            this.to = to;
            this.sleepMillis = sleepMillis;
            this.sharedFinishedList = sharedFinishedList;
        }

        @Override
        public void run() {
            status = "RUNNING";
            if (sleepMillis > 0) { // only used by the monitoring demo so thread states can be observed
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            long t0 = System.nanoTime();
            partial = Statistics.computeRange(sharedData, from, to);
            elapsedNanos = System.nanoTime() - t0; // compute time of this worker only (sleep excluded)

            // Shared result collection: several workers may finish at the same time,
            // so adding to the list is protected by a synchronized block.
            synchronized (sharedFinishedList) {
                sharedFinishedList.add(this);
            }
            status = "FINISHED";
        }

        public int getWorkerId() {
            return workerId;
        }

        public int getFrom() {
            return from;
        }

        public int getTo() {
            return to;
        }

        public String getStatus() {
            return status;
        }

        public long getElapsedNanos() {
            return elapsedNanos;
        }

        public Statistics.Partial getPartial() {
            return partial;
        }
    }

    /** Result of one parallel run. */
    public static class Outcome {
        public Statistics.Partial partial;
        public long nanos;
        public int workersUsed;
        public List<StatWorker> workers = new ArrayList<>();
        // Each row: name, id, partition, items, priority, status, thread state, alive
        public List<String[]> snapshot1 = new ArrayList<>(); // before start()
        public List<String[]> snapshot2 = new ArrayList<>(); // right after start()
        public List<String[]> snapshot3 = new ArrayList<>(); // after join()
    }

    /**
     * priorityMode: 0 = all NORM_PRIORITY, 1 = cycle MIN / NORM / MAX, 2 = all MAX_PRIORITY.
     * sleepMillis: 0 for real measurements; greater than 0 only for the monitoring demo.
     */
    public static Outcome runParallel(double[] data, int requestedWorkers, int priorityMode,
                                      long sleepMillis, boolean captureMonitor) throws InterruptedException {
        long start = System.nanoTime();
        int workerCount = Math.max(1, Math.min(requestedWorkers, data.length)); // never more workers than values

        List<StatWorker> finished = new ArrayList<>();
        List<StatWorker> workers = new ArrayList<>();

        int base = data.length / workerCount;
        int remainder = data.length % workerCount;
        int from = 0;
        for (int i = 0; i < workerCount; i++) {
            int size = base + (i < remainder ? 1 : 0); // spread the remainder over the first workers
            StatWorker w = new StatWorker(i + 1, data, from, from + size, sleepMillis, finished);
            w.setPriority(priorityFor(priorityMode, i));
            workers.add(w);
            from += size;
        }

        Outcome outcome = new Outcome();
        if (captureMonitor) {
            outcome.snapshot1 = snapshotRows(workers);
        }

        for (StatWorker w : workers) {
            w.start();
        }
        if (captureMonitor) {
            outcome.snapshot2 = snapshotRows(workers);
        }

        for (StatWorker w : workers) {
            w.join(); // master waits for every worker
        }

        List<StatWorker> sorted;
        synchronized (finished) {
            sorted = new ArrayList<>(finished);
        }
        sorted.sort(Comparator.comparingInt(StatWorker::getWorkerId));

        Statistics.Partial combined = new Statistics.Partial();
        for (StatWorker w : sorted) {
            combined = Statistics.Partial.merge(combined, w.getPartial());
        }

        outcome.partial = combined;
        outcome.nanos = System.nanoTime() - start;
        outcome.workersUsed = workerCount;
        outcome.workers = workers;

        if (captureMonitor) {
            outcome.snapshot3 = snapshotRows(workers);
        }
        return outcome;
    }

    private static int priorityFor(int mode, int index) {
        if (mode == 1) {
            int[] cycle = { Thread.MIN_PRIORITY, Thread.NORM_PRIORITY, Thread.MAX_PRIORITY };
            return cycle[index % 3];
        }
        if (mode == 2) {
            return Thread.MAX_PRIORITY;
        }
        return Thread.NORM_PRIORITY;
    }

    private static String priorityName(int p) {
        if (p == Thread.MIN_PRIORITY) {
            return "1 (MIN)";
        }
        if (p == Thread.NORM_PRIORITY) {
            return "5 (NORM)";
        }
        if (p == Thread.MAX_PRIORITY) {
            return "10 (MAX)";
        }
        return String.valueOf(p);
    }

    private static List<String[]> snapshotRows(List<StatWorker> workers) {
        List<String[]> rows = new ArrayList<>();
        for (StatWorker w : workers) {
            rows.add(new String[] {
                    w.getName(),
                    String.valueOf(w.getWorkerId()),
                    "[" + w.getFrom() + ", " + w.getTo() + ")",
                    String.valueOf(w.getTo() - w.getFrom()),
                    priorityName(w.getPriority()),
                    w.getStatus(),
                    w.getState().toString(),
                    w.isAlive() ? "Alive" : "Not alive" });
        }
        return rows;
    }

    // ------------------------------------------------------------------------------------
    // Synchronization demonstration
    // ------------------------------------------------------------------------------------

    /** Two counters: one unprotected, one protected by synchronized. */
    public static class SharedCounter {
        private int unsafeValue = 0;
        private int safeValue = 0;

        public void incrementUnsafe() {
            unsafeValue++; // read, add, write: three steps that threads can interleave (race condition)
        }

        public synchronized void incrementSafe() {
            safeValue++; // only one thread at a time can execute this method
        }

        public int getUnsafe() {
            return unsafeValue;
        }

        public synchronized int getSafe() {
            return safeValue;
        }
    }

    /** Returns { expected value, result without synchronization, result with synchronization }. */
    public static long[] synchronizationCounts(int threadCount, int incrementsPerThread) throws InterruptedException {
        SharedCounter counter = new SharedCounter();

        Thread[] unsafeThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            unsafeThreads[i] = new Thread(() -> {
                for (int k = 0; k < incrementsPerThread; k++) {
                    counter.incrementUnsafe();
                }
            }, "Unsafe-" + (i + 1));
        }
        for (Thread t : unsafeThreads) {
            t.start();
        }
        for (Thread t : unsafeThreads) {
            t.join();
        }

        Thread[] safeThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            safeThreads[i] = new Thread(() -> {
                for (int k = 0; k < incrementsPerThread; k++) {
                    counter.incrementSafe();
                }
            }, "Safe-" + (i + 1));
        }
        for (Thread t : safeThreads) {
            t.start();
        }
        for (Thread t : safeThreads) {
            t.join();
        }

        long expected = (long) threadCount * incrementsPerThread;
        return new long[] { expected, counter.getUnsafe(), counter.getSafe() };
    }

    // ------------------------------------------------------------------------------------
    // Multiple arrays demonstration
    // ------------------------------------------------------------------------------------

    public static String multipleArraysDemo(int size) throws InterruptedException {
        Random random = new Random(42);
        double[] a = new double[size];
        double[] b = new double[size];
        double[] c = new double[size];
        for (int i = 0; i < size; i++) {
            a[i] = random.nextDouble() * 100.0;
            b[i] = random.nextDouble() * 100.0;
            c[i] = random.nextDouble() * 100.0;
        }

        double[] sumA = new double[1];
        double[] avgB = new double[1];
        double[] maxC = new double[1];

        Thread threadA = new Thread(() -> {
            double s = 0.0;
            for (double v : a) {
                s += v;
            }
            sumA[0] = s;
        }, "Thread-A-Sum");

        Thread threadB = new Thread(() -> {
            double s = 0.0;
            for (double v : b) {
                s += v;
            }
            avgB[0] = s / b.length;
        }, "Thread-B-Average");

        Thread threadC = new Thread(() -> {
            double m = Double.NEGATIVE_INFINITY;
            for (double v : c) {
                if (v > m) {
                    m = v;
                }
            }
            maxC[0] = m;
        }, "Thread-C-Maximum");

        threadA.start();
        threadB.start();
        threadC.start();
        threadA.join();
        threadB.join();
        threadC.join();

        // Sequential check
        double checkSum = 0.0;
        double checkBSum = 0.0;
        double checkMax = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < size; i++) {
            checkSum += a[i];
            checkBSum += b[i];
            if (c[i] > checkMax) {
                checkMax = c[i];
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("MULTIPLE ARRAYS DEMONSTRATION (three arrays, three threads)\n");
        sb.append(String.format(Locale.US, "Each array has %d random values between 0 and 100 (fixed seed 42).%n%n", size));
        sb.append(String.format(Locale.US, "%-18s %-20s %-16s %s%n", "Thread", "Task", "Thread result", "Sequential check"));
        sb.append(String.format(Locale.US, "%-18s %-20s %-16.4f %.4f%n", "Thread-A-Sum", "Array A: sum", sumA[0], checkSum));
        sb.append(String.format(Locale.US, "%-18s %-20s %-16.4f %.4f%n", "Thread-B-Average", "Array B: average", avgB[0], checkBSum / size));
        sb.append(String.format(Locale.US, "%-18s %-20s %-16.4f %.4f%n", "Thread-C-Maximum", "Array C: maximum", maxC[0], checkMax));
        sb.append("\nThe three threads work on different arrays, so they do not need synchronization.\n");
        return sb.toString();
    }

    // ------------------------------------------------------------------------------------
    // OpenMP-style demonstration (Java threads, NOT native OpenMP)
    // ------------------------------------------------------------------------------------

    public static String openMpStyleDemo(int threadCount) throws InterruptedException {
        StringBuffer output = new StringBuffer(); // StringBuffer is thread-safe
        output.append("OPENMP-STYLE DEMONSTRATION USING JAVA THREADS\n");
        output.append("Java does not implement native OpenMP. This is the equivalent idea of\n");
        output.append("'#pragma omp parallel num_threads(").append(threadCount).append(")' written with java.lang.Thread.\n\n");

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                String message = "Hello from " + Thread.currentThread().getName()
                        + " (thread " + (id + 1) + " of " + threadCount + ")";
                System.out.println(message);
                output.append(message).append("\n");
            }, "OMP-Style-" + (i + 1));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        output.append("\nThe order of the lines can change between runs because the threads run concurrently.\n");
        return output.toString();
    }
}