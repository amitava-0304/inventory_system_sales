import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DBConnectionPool {
    private static final HikariDataSource ds;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/billing_system");
        config.setUsername("root");
        config.setPassword("root");
        config.setMaximumPoolSize(4); // match thread count
        config.setMinimumIdle(2);
        config.setIdleTimeout(30000);
        config.setMaxLifetime(60000);
        ds = new HikariDataSource(config);
    }

    public static DataSource getDataSource() {
        return ds;
    }
}
