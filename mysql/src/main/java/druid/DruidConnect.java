package druid;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DruidConnect {
    public static void main(String[] args) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            // 2. 建立连接
            conn = JDBCUtils.getConnection();
            // 3. 操作数据
            String sql = "select * from user;";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String username = rs.getString("username");
                String password = rs.getString("password");
                String email = rs.getString("email");
                System.out.println(id + " : " + username + " : " + password + " : " + email);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 4. 释放资源
            JDBCUtils.release(conn, stmt, rs);
        }
    }
}