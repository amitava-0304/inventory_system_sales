import java.sql.*;
import java.util.concurrent.*;
import com.zaxxer.hikari.*;

public class BillingSystemBenchmark_Thread {

    static final String[] QUERIES = {
        "SELECT SUM(SalesDollars) AS TotalSales FROM inventory_sales",
        "SELECT Brand, SUM(SalesDollars) AS Total FROM inventory_sales GROUP BY Brand ORDER BY Total DESC LIMIT 5",
        "SELECT Store, SUM(SalesQuantity) AS Quantity FROM inventory_sales GROUP BY Store ORDER BY Quantity DESC LIMIT 5",
        "SELECT VendorName, SUM(SalesDollars) AS Total FROM inventory_sales GROUP BY VendorName ORDER BY Total DESC LIMIT 5",
        "SELECT COUNT(*) FROM inventory_sales",
        "SELECT MAX(SalesPrice) FROM inventory_sales",
        "SELECT MIN(SalesPrice) FROM inventory_sales",
        "SELECT AVG(SalesQuantity) FROM inventory_sales",
        "SELECT Classification, COUNT(*) FROM inventory_sales GROUP BY Classification",
        "SELECT SalesDate, SUM(SalesDollars) FROM inventory_sales GROUP BY SalesDate ORDER BY SalesDate DESC LIMIT 5"
    };

    public static void main(String[] args) throws Exception {
        for (int threads : new int[]{2, 4, 8, 16}) {
            System.out.println("\n========= THREAD COUNT: " + threads + " =========");

            long time1 = runSingleThreaded();
            System.out.println("🧵 Single-threaded Time: " + time1 + " ms");

            long time2 = runMultiThreadedWithoutPooling(threads);
            System.out.println("🔁 Multi-threaded (No Pooling) Time: " + time2 + " ms");

            long time3 = runMultiThreadedWithPooling(threads);
            System.out.println("🔁⚡ Multi-threaded (With Pooling) Time: " + time3 + " ms");
        }
    }

    static long runSingleThreaded() {
        long start = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/billing_system", "root", "root");
             Statement stmt = conn.createStatement()) {
            for (String query : QUERIES) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    printResult(rs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - start;
    }

    static long runMultiThreadedWithoutPooling(int threadCount) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        long start = System.currentTimeMillis();

        for (String query : QUERIES) {
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/billing_system", "root", "");
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    printResult(rs);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - start;
    }

    static long runMultiThreadedWithPooling(int threadCount) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        long start = System.currentTimeMillis();

        for (String query : QUERIES) {
            executor.submit(() -> {
                try (Connection conn = DBConnectionPool.getDataSource().getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    printResult(rs);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - start;
    }

    static void printResult(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= cols; i++) {
                System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
            }
            System.out.println();
        }
    }
}
