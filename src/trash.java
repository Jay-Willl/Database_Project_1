import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class trash {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");

        String test = "2020/2/2";
        System.out.println(test.replace('/', '-'));

        System.out.println(simpleDateFormat.parse("2020/2/2").toString());
        System.out.println(Date.valueOf("2020-2-2"));
    }



    public static void initialize_o4() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/project_1";
        String username = "";
        String password = "";
        Connection conn = DriverManager.getConnection(url,username,password);
        Statement statement = conn.createStatement();
        String command = "truncate table center cascade;" +
                "truncate table contract cascade;" +
                "truncate table enterprise cascade;" +
                "truncate table model cascade;" +
                "truncate table orders cascade;" +
                "truncate table product cascade;" +
                "truncate table sale_man cascade;";
        statement.execute(command);
        statement.close();
    }

    public static void initialize_pg() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/project_1";
        String username = "";
        String password = "";
        Connection conn = DriverManager.getConnection(url,username,password);
        Statement statement = conn.createStatement();
        String command = "truncate table center cascade;" +
                "truncate table contract cascade;" +
                "truncate table enterprise cascade;" +
                "truncate table model cascade;" +
                "truncate table orders cascade;" +
                "truncate table product cascade;" +
                "truncate table sale_man cascade;";
        statement.execute(command);
        statement.close();
    }

    public static void initialize_my() throws SQLException {
        String url = "jdbc:mysql://localhost:3306/project_1";
        String username = "";
        String password = "";
        Connection conn = DriverManager.getConnection(url,username,password);
        Statement statement = conn.createStatement();
        String command = "drop table center;" +
                "drop table contract;" +
                "drop table enterprise;" +
                "drop table model;" +
                "drop table orders;" +
                "drop table product;" +
                "drop table sale_man;";
        statement.execute(command);
        statement.close();
    }
}
