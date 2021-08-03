package druid;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCUtils {

    // 属性

    private static final DataSource dataSource;

/*
    静态代码块的特点 :
    1. 类加载时, 自动执行静态代码块.
    2. 静态静态块仅会被执行一次, 因为类只会被虚拟机加载一次.
 */

    static {

        Properties prop = new Properties();

        try {

            // 加载配置文件

            prop.load(new FileReader("druid.properties"));

            // 创建数据库连接池

            dataSource = DruidDataSourceFactory.createDataSource(prop);

        } catch (Exception e) {

            throw new RuntimeException("连接池初始化失败!");

        }

    }

    //获取druid连接池对象

    public static DataSource getDataSource() {

        return dataSource;

    }

    // 建立连接

    public static Connection getConnection() throws SQLException {

        return dataSource.getConnection();

    }

    // 释放资源

    public static void release(Connection conn, Statement stmt, ResultSet rs) {

        if (rs != null) {

            try {

                rs.close();

            } catch (SQLException e) {

                e.printStackTrace();

            }

            rs = null;

        }

        release(conn, stmt);

    }

    public static void release(Connection conn, Statement stmt) {

        if (stmt != null) {

            try {

                stmt.close();

            } catch (SQLException e) {

                e.printStackTrace();

            }

            stmt = null;

        }

        if (conn != null) {

            try {

                conn.close();

            } catch (SQLException e) {

                // e.printStackTrace();  ignore 忽略.

            }

            conn = null;    // 目的: 让 conn 对象尽早被回收.

        }

    }

}
