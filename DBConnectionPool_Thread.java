import com.zaxxer.hikari.*;

public class DBConnectionPool_Thread {
    private static final HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/billing_system");
        config.setUsername("root");
        config.setPassword("root");
        config.setMaximumPoolSize(16);  // Allow up to 16 threads
        dataSource = new HikariDataSource(config);
    }

    public static HikariDataSource getDataSource() {
        return dataSource;
    }
}
