import java.io.*;
import java.sql.*;
import java.util.*;

public class CSVtoMySQLImporter {

    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/billing_system";
        String username = "root";
        String password = "root"; // change this
        String csvFilePath = "data/SalesFINAL12312016.csv"; // make sure this path is correct

        int batchSize = 1000;

        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {

            Class.forName("com.mysql.cj.jdbc.Driver");

            connection.setAutoCommit(false);

            String sql = "INSERT INTO inventory_sales (InventoryId, Store, Brand, Description, Size, SalesQuantity, SalesDollars, SalesPrice, SalesDate, Volume, Classification, ExciseTax, VendorNo, VendorName) "
                       + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText;

            // Skip header
            lineReader.readLine();

            int count = 0;

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",", -1); // use -1 to include empty strings

                statement.setString(1, data[0]);
                statement.setString(2, data[1]);
                statement.setString(3, data[2]);
                statement.setString(4, data[3]);
                statement.setString(5, data[4]);
                statement.setInt(6, parseIntSafe(data[5]));
                statement.setBigDecimal(7, parseBigDecimalSafe(data[6]));
                statement.setBigDecimal(8, parseBigDecimalSafe(data[7]));
                statement.setDate(9, parseDateSafe(data[8]));
                statement.setString(10, data[9]);
                statement.setString(11, data[10]);
                statement.setBigDecimal(12, parseBigDecimalSafe(data[11]));
                statement.setString(13, data[12]);
                statement.setString(14, data[13]);

                statement.addBatch();

                if (++count % batchSize == 0) {
                    statement.executeBatch();
                }
            }

            lineReader.close();
            statement.executeBatch();
            connection.commit();
            connection.close();

            System.out.println("Data has been successfully inserted.");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static int parseIntSafe(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            return 0;
        }
    }

    private static java.math.BigDecimal parseBigDecimalSafe(String value) {
        try {
            return new java.math.BigDecimal(value.trim());
        } catch (Exception e) {
            return java.math.BigDecimal.ZERO;
        }
    }

    private static java.sql.Date parseDateSafe(String value) {
        try {
            return java.sql.Date.valueOf(value.trim());
        } catch (Exception e) {
            return null;
        }
    }
}
