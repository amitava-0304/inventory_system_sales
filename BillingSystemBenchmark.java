import java.sql.*;
import java.util.concurrent.*;
import javax.sql.DataSource;

public class BillingSystemBenchmark {

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

    static final int THREAD_COUNT = 4; // You can change this

    public static void main(String[] args) throws Exception {
        long t1 = System.currentTimeMillis();
        runSingleThreaded();
        long t2 = System.currentTimeMillis();
        System.out.println("\nSingle-threaded Time: " + (t2 - t1) + " ms");

        long t3 = System.currentTimeMillis();
        runMultiThreadedNoPooling();
        long t4 = System.currentTimeMillis();
        System.out.println("\nMulti-threaded (No Pooling) Time: " + (t4 - t3) + " ms");

        long t5 = System.currentTimeMillis();
        runMultiThreadedWithPooling();
        long t6 = System.currentTimeMillis();
        System.out.println("\nMulti-threaded (With Pooling) Time: " + (t6 - t5) + " ms");
    }

    static void runSingleThreaded() {
        for (String query : QUERIES) {
            try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/billing_system", "root", "root");
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("Running Query (Single-threaded): " + query);
                printResult(rs);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    static void runMultiThreadedNoPooling() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        for (String query : QUERIES) {
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/billing_system", "root", "root");
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    System.out.println("Running Query (No Pooling): " + query);
                    printResult(rs);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.MINUTES);
    }

    static void runMultiThreadedWithPooling() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        DataSource ds = DBConnectionPool.getDataSource();

        for (String query : QUERIES) {
            executor.submit(() -> {
                try (Connection conn = ds.getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    System.out.println("Running Query (With Pooling): " + query);
                    printResult(rs);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.MINUTES);
    }

    static void printResult(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + " | ");
            }
            System.out.println();
        }
    }
}
