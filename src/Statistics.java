import java.util.Arrays;
import java.util.Locale;

/**
 * All statistical formulas.
 *
 * Partial = count, sum, mean, M2 (sum of squared differences from the mean), min, max.
 * A Partial can be computed for any part of the data and merged exactly (Chan's method),
 * which is what makes the parallel and distributed versions mathematically correct.
 */
public class Statistics {

    public static class Partial {
        public long count = 0;
        public double sum = 0.0;
        public double mean = 0.0;
        public double m2 = 0.0;
        public double min = Double.POSITIVE_INFINITY;
        public double max = Double.NEGATIVE_INFINITY;

        /** Welford's one-pass update. */
        public void add(double x) {
            count++;
            sum += x;
            double delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
            if (x < min) {
                min = x;
            }
            if (x > max) {
                max = x;
            }
        }

        /** Combines two partial results into one (does not average variances). */
        public static Partial merge(Partial a, Partial b) {
            Partial r = new Partial();
            if (a.count == 0) {
                copyInto(b, r);
                return r;
            }
            if (b.count == 0) {
                copyInto(a, r);
                return r;
            }
            r.count = a.count + b.count;
            r.sum = a.sum + b.sum;
            double delta = b.mean - a.mean;
            r.mean = a.mean + delta * ((double) b.count / r.count);
            r.m2 = a.m2 + b.m2 + delta * delta * ((double) a.count * b.count / r.count);
            r.min = Math.min(a.min, b.min);
            r.max = Math.max(a.max, b.max);
            return r;
        }

        private static void copyInto(Partial from, Partial to) {
            to.count = from.count;
            to.sum = from.sum;
            to.mean = from.mean;
            to.m2 = from.m2;
            to.min = from.min;
            to.max = from.max;
        }
    }

    /** Final statistics shown to the user. */
    public static class Result {
        public long count;
        public double sum;
        public double mean;
        public double median;
        public double min;
        public double max;
        public double range;
        public double populationVariance;
        public double sampleVariance;
        public double populationStdDev;
        public double sampleStdDev;

        public static Result fromPartial(Partial p, double median) {
            Result r = new Result();
            r.count = p.count;
            r.sum = p.sum;
            r.mean = p.mean;
            r.median = median;
            r.min = p.min;
            r.max = p.max;
            r.range = p.max - p.min;
            r.populationVariance = (p.count > 0) ? p.m2 / p.count : Double.NaN;
            r.sampleVariance = (p.count > 1) ? p.m2 / (p.count - 1) : Double.NaN;
            r.populationStdDev = Math.sqrt(r.populationVariance);
            r.sampleStdDev = Math.sqrt(r.sampleVariance);
            return r;
        }

        public String toText() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Locale.US, "%-32s %d%n", "Count", count));
            sb.append(line("Sum", sum));
            sb.append(line("Mean", mean));
            sb.append(line("Median", median));
            sb.append(line("Minimum", min));
            sb.append(line("Maximum", max));
            sb.append(line("Range (max - min)", range));
            sb.append(line("Population variance", populationVariance));
            sb.append(line("Sample variance", sampleVariance));
            sb.append(line("Population standard deviation", populationStdDev));
            sb.append(line("Sample standard deviation", sampleStdDev));
            return sb.toString();
        }

        private static String line(String label, double value) {
            return String.format(Locale.US, "%-32s %.6f%n", label, value);
        }
    }

    /** Processes data[from] up to data[to - 1] in one pass. */
    public static Partial computeRange(double[] data, int from, int to) {
        Partial p = new Partial();
        for (int i = from; i < to; i++) {
            p.add(data[i]);
        }
        return p;
    }

    /** Median needs the sorted data, so it cannot be merged from partial values. */
    public static double median(double[] data) {
        if (data.length == 0) {
            return Double.NaN;
        }
        double[] copy = Arrays.copyOf(data, data.length);
        Arrays.sort(copy);
        int n = copy.length;
        if (n % 2 == 1) {
            return copy[n / 2];
        }
        return (copy[n / 2 - 1] + copy[n / 2]) / 2.0;
    }

    /** True if two partial results agree within a relative tolerance of 1e-9. */
    public static boolean matches(Partial a, Partial b) {
        return a.count == b.count
                && close(a.sum, b.sum)
                && close(a.mean, b.mean)
                && close(a.m2, b.m2)
                && close(a.min, b.min)
                && close(a.max, b.max);
    }

    private static boolean close(double x, double y) {
        double scale = Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)));
        return Math.abs(x - y) <= 1e-9 * scale;
    }

    public static double meanOf(double[] v) {
        double s = 0.0;
        for (double d : v) {
            s += d;
        }
        return s / v.length;
    }

    /** Covariance = sum((x - meanX) * (y - meanY)) / n (population) or / (n - 1) (sample). */
    public static double covariance(double[] x, double[] y, boolean sample) {
        int n = x.length;
        if (n == 0 || (sample && n < 2)) {
            return Double.NaN;
        }
        double mx = meanOf(x);
        double my = meanOf(y);
        double cross = 0.0;
        for (int i = 0; i < n; i++) {
            cross += (x[i] - mx) * (y[i] - my);
        }
        return cross / (sample ? n - 1 : n);
    }

    /** Pearson correlation = cross / (sqrt(sumSquaredX) * sqrt(sumSquaredY)). */
    public static double correlation(double[] x, double[] y) {
        int n = x.length;
        if (n < 2) {
            return Double.NaN;
        }
        double mx = meanOf(x);
        double my = meanOf(y);
        double cross = 0.0;
        double sxx = 0.0;
        double syy = 0.0;
        for (int i = 0; i < n; i++) {
            double dx = x[i] - mx;
            double dy = y[i] - my;
            cross += dx * dy;
            sxx += dx * dx;
            syy += dy * dy;
        }
        double denominator = Math.sqrt(sxx) * Math.sqrt(syy);
        if (denominator == 0.0) {
            return Double.NaN;
        }
        return cross / denominator;
    }
}