import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DatabaseIO {
    static String url = "jdbc:postgresql://localhost:5432/project_1";
    static String username = "blank";
    static String password = "411128";

    static String url_my = "jdbc:mysql://localhost:3306/project_1";
    static String username_my = "root";
    static String password_my = "Betelgeuse4";
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement statement = conn.createStatement();
        String command1 = "select product_code from orders where salesman_number = 11110111;";
        String command2 = "select distinct t1.salesman_number, t2.quantity from (select salesman_number, quantity from orders where lodgement_date < to_date('2000-02-01', 'yyyy-mm-dd')) t1, (select salesman_number, quantity from orders where quantity > 999) t2 where t1.salesman_number = t2.salesman_number;";
        String command4 = "update orders set salesman_number = 300991 where salesman_number = 11110111;";
        String command5 = "delete from orders where salesman_number = 300991;";
        String command6 = "insert into orders (orders_id, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number) values (550000, 'I36Z941', 800, '2010/4/12', '2010/4/21', 'CSE0004999', 12111601);";

        long time1 = System.nanoTime();
        statement.execute(command1);
        long time2 = System.nanoTime();
        System.out.println("Task1 takes : "+((double)(time2-time1)/1000000)+" ms (Postgresql)");

        time1 = System.nanoTime();
        statement.execute(command2);
        time2 = System.nanoTime();
        System.out.println("Task2 takes : "+((double)(time2-time1)/1000000)+" ms (Postgresql)");

        time1 = System.nanoTime();
        statement.execute(command4);
        time2 = System.nanoTime();
        System.out.println("Task4 takes : "+((double)(time2-time1)/1000000)+" ms (Postgresql)");

        time1 = System.nanoTime();
        statement.execute(command5);
        time2 = System.nanoTime();
        System.out.println("Task5 takes : "+((double)(time2-time1)/1000000)+" ms (Postgresql)");

        time1 = System.nanoTime();
        statement.execute(command6);
        time2 = System.nanoTime();
        System.out.println("Task6 takes : "+((double)(time2-time1)/1000000)+" ms (Postgresql)");

        ////////////////
        Connection conn_my = DriverManager.getConnection(url_my, username_my, password_my);
        statement = conn_my.createStatement();

        String command1_my = "select product_code from orders where salesman_number = 11110111;";
        String command2_my = "select distinct t1.salesman_number, t2.quantity from (select salesman_number, quantity from orders where lodgement_date < date('2000-02-01')) t1, (select salesman_number, quantity from orders where quantity > 999) t2 where t1.salesman_number = t2.salesman_number;";
        String command4_my = "update orders set salesman_number = 300991 where salesman_number = 11110111;";
        String command5_my = "delete from orders where salesman_number = 300991;";
        String command6_my = "insert into orders (orders_id, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number) values (550000, 'I36Z941', 800, '2010/4/12', '2010/4/21', 'CSE0004999', 12111601);";

        time1 = System.nanoTime();
        statement.execute(command1_my);
        time2 = System.nanoTime();
        System.out.println("Task1 takes : "+((double)(time2-time1)/1000000)+" ms (MySQL)");

        time1 = System.nanoTime();
        statement.execute(command2_my);
        time2 = System.nanoTime();
        System.out.println("Task2 takes : "+((double)(time2-time1)/1000000)+" ms (MySQL)");

        time1 = System.nanoTime();
        statement.execute(command4_my);
        time2 = System.nanoTime();
        System.out.println("Task4 takes : "+((double)(time2-time1)/1000000)+" ms (MySQL)");

        time1 = System.nanoTime();
        statement.execute(command5_my);
        time2 = System.nanoTime();
        System.out.println("Task5 takes : "+((double)(time2-time1)/1000000)+" ms (MySQL)");

        time1 = System.nanoTime();
        statement.execute(command6_my);
        time2 = System.nanoTime();
        System.out.println("Task6 takes : "+((double)(time2-time1)/1000000)+" ms (MySQL)");
    }
}
