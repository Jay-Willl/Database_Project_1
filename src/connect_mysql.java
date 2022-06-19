import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class connect_mysql {
    static Connection conn = null;
    static PreparedStatement  stmt1 = null;
    static PreparedStatement  stmt2 = null;
    static PreparedStatement  stmt3 = null;
    static PreparedStatement  stmt4 = null;
    static PreparedStatement  stmt5 = null;
    static PreparedStatement  stmt6 = null;
    static PreparedStatement  stmt7 = null;
    static PreparedStatement  stmtx = null;

    public static void exe() throws SQLException, IOException {
        // order: center, enterprise, contract, product, sale_man, model, order
        long     start;
        long     mid;
        long     mid2;
        long     end;
        int      count = 0;
        long     s1;
        long     s2;
        String   line;
        String[] parts;
        String   contract_number;
        String   client_enterprise;
        String   supply_center;
        String   country;
        String   city;
        String   industry;
        String   product_code;
        String   product_name;
        String   product_model;
        int      unit_price;
        int      quantity;
        Date   contract_date;
        Date   estimated_delivery_date;
        Date   lodgement_date;
        String   director;
        String   salesman;
        int      salesman_number;
        String   gender;
        int      age;
        long      mobile_phone;
        int      cnt = 1;
        Map<String, Integer> lookUp = new HashMap<>();

        start = System.currentTimeMillis();
        Process process;
        try{
            String[] command = new String[]{"/Users/blank/IdeaProjects/DB_project_1/venv/bin/python","/Users/blank/IdeaProjects/DB_project_1/src/rinse/center.py"};
            process = Runtime.getRuntime().exec(command);
            BufferedReader ine = new BufferedReader((new InputStreamReader((process.getErrorStream()))));
            String xlinee;
            while((xlinee = ine.readLine()) != null){
                System.out.println(xlinee);
            }
            ine.close();
            process.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        mid = System.currentTimeMillis();
        System.out.println("    Pretreatment time : " + (mid - start) + " ms");

        String url = "jdbc:mysql://localhost:3306/project_1";
        String username = "root";
        String password = "Betelgeuse4";

        String work_dir = "/Users/blank/IdeaProjects/DB_project_1/src";
        String source_dir = "/Users/blank/IdeaProjects/DB_project_1/src/splits";
        BufferedReader infile;

        conn = DriverManager.getConnection(url,username,password);
        conn.setAutoCommit(false);

        stmt1 = conn.prepareStatement(
                "insert into center(center_id, supply_center, industry)"
                        + " values(?,?,?)");
        stmt2 = conn.prepareStatement(
                "insert into enterprise(enterprise_id, client_enterprise, country, city, center_id)"
                        + " values(?,?,?,?,?)");
        stmt3 = conn.prepareStatement(
                "insert into contract(contract_id, contract_number, director, contract_date, client_enterprise)"
                        + " values(?,?,?,?,?)");
        stmt4 = conn.prepareStatement(
                "insert into sale_man(salesman_id, salesman_number, salesman, gender, age, mobile_phone, center_id)"
                        + " values(?,?,?,?,?,?,?)");
        stmt5 = conn.prepareStatement(
                "insert into product(product_id, product_code, product_name)"
                        + " values(?,?,?)");
        stmt6 = conn.prepareStatement(
                "insert into orders(orders_id, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number)"
                        + " values(?,?,?,?,?,?,?)");
        stmt7 = conn.prepareStatement(
                "insert into model(model_id, product_model, unit_price, product_code)"
                        + " values(?,?,?,?)");

        stmtx = conn.prepareStatement(
                "insert into orders_4(orders_id, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number)"
                        + " values(?,?,?,?,?,?,?)");

        mid2 = System.currentTimeMillis();
        System.out.println("    Connect time : " + (mid2 - mid) + "ms");

        // center
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/center_sub.csv"));
        infile.skip(23);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                supply_center = parts[0];
                industry = parts[1];
                loadCenter(cnt, supply_center, industry);
                lookUp.put(parts[0]+parts[1], cnt);
            }
            cnt++;
        }
        count += cnt;
        stmt1.executeBatch();
        stmt1.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table center has been imported, time: "+(s2-s1)+" ms");

        // enterprise
        cnt = 100001;
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/enterprise_sub.csv"));
        infile.skip(54);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                client_enterprise = parts[0];
                country = parts[1];
                city = parts[2];
                supply_center = parts[3];
                industry = parts[4];
                int id = lookUp.get(supply_center+industry);
                loadEnterprise(cnt, client_enterprise, country, city, id);
            }
            cnt++;
        }
        count += (cnt-100000);
        stmt2.executeBatch();
        stmt2.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table enterprise has been imported, time: "+(s2-s1)+" ms");

        // contract
        cnt = 200001;
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/contract_sub.csv"));
        infile.skip(57);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                contract_number = parts[0];
                director = parts[1];
                contract_date = Date.valueOf(parts[2]);
                client_enterprise = parts[3];
                loadContract(cnt, contract_number, director, contract_date, client_enterprise);
            }
            cnt++;
        }
        count += (cnt-200000);
        stmt3.executeBatch();
        stmt3.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table contract has been imported, time: "+(s2-s1)+" ms");

        // salesman
        cnt = 300001;
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/salesman_sub.csv"));
        infile.skip(72);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                salesman_number = Integer.parseInt(parts[0]);
                salesman = parts[1];
                gender = parts[2];
                age = Integer.parseInt(parts[3]);
                mobile_phone = Long.parseLong(parts[4]);
                supply_center = parts[5];
                industry = parts[6];
                int id = lookUp.get(parts[5]+parts[6]);
                loadSale_man(cnt, salesman_number, salesman, gender, age, mobile_phone, id);
            }
            cnt++;
        }
        count += (cnt-300000);
        stmt4.executeBatch();
        stmt4.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table salesman has been imported, time: "+(s2-s1)+" ms");

        // product
        cnt = 400001;
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/product_sub.csv"));
        infile.skip(26);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                product_code = parts[0];
                product_name = parts[1];
                loadProduct(cnt, product_code, product_name);
            }
            cnt++;
        }
        count += (cnt-400000);
        stmt5.executeBatch();
        stmt5.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table product has been imported, time: "+(s2-s1)+" ms");

        // order
        cnt = 500001;
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/orders_sub.csv"));
        infile.skip(93);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                product_code = parts[0];
                quantity = Integer.parseInt(parts[1]);
                if(!Objects.equals(parts[2], "")){
                    estimated_delivery_date = Date.valueOf(parts[2]);
                }else{
                    estimated_delivery_date = null;
                }
                if(!Objects.equals(parts[3], "")){
                    lodgement_date = Date.valueOf(parts[3]);
                }else{
                    lodgement_date = null;
                }
                contract_number = parts[4];
                salesman_number = Integer.parseInt(parts[5]);
                loadOrders(cnt, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number);
            }
            cnt++;
        }
        count += (cnt-500000);
        stmt6.executeBatch();
        stmt6.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table order has been imported, time: "+(s2-s1)+" ms");

        // model
        cnt = 600001;
        s1 = System.currentTimeMillis();
        infile = new BufferedReader(new FileReader(source_dir + "/model_sub.csv"));
        infile.skip(34);
        while((line = infile.readLine()) != null){
            parts = line.split(",");
            if(parts.length > 1){
                product_model = parts[0];
                unit_price = Integer.parseInt(parts[1]);
                product_code = parts[2];
                loadModel(cnt, product_model, unit_price, product_code);
            }
            cnt++;
        }
        count += (cnt-600000);
        stmt7.executeBatch();
        stmt7.close();
        s2 = System.currentTimeMillis();
        System.out.println("        table model has been imported, time: "+(s2-s1)+" ms");

        conn.commit();

        end = System.currentTimeMillis();
        System.out.println("Import time : " + (end - mid2) + " ms");
        System.out.println("Total time : " + (end - start) + " ms");
    }

    private static void loadCenter(int cnt, String supply_center, String industry) throws SQLException {
        if (conn != null) {
            stmt1.setInt(1, cnt);
            stmt1.setString(2, supply_center);
            stmt1.setString(3, industry);
            stmt1.addBatch();
        }
    }

    private static void loadEnterprise(int cnt, String client_enterprise, String country, String city, int id) throws SQLException {
        if (conn != null) {
            stmt2.setInt(1, cnt);
            stmt2.setString(2, client_enterprise);
            stmt2.setString(3, country);
            stmt2.setString(4, city);
            stmt2.setInt(5, id);
            stmt2.addBatch();
        }
    }

    private static void loadContract(int cnt, String contract_number, String director, Date contract_date, String client_enterprise) throws SQLException {
        if (conn != null) {
            stmt3.setInt(1, cnt);
            stmt3.setString(2, contract_number);
            stmt3.setString(3, director);
            stmt3.setDate(4, contract_date);
            stmt3.setString(5, client_enterprise);
            stmt3.addBatch();
        }
    }

    private static void loadSale_man(int cnt, int salesman_number, String salesman, String gender, int age, long mobile_phone, int id) throws SQLException {
        if (conn != null) {
            stmt4.setInt(1, cnt);
            stmt4.setInt(2, salesman_number);
            stmt4.setString(3, salesman);
            stmt4.setString(4, gender);
            stmt4.setInt(5, age);
            stmt4.setLong(6, mobile_phone);
            stmt4.setInt(7, id);
            stmt4.addBatch();
        }
    }

    private static void loadProduct(int cnt, String product_code, String product_name) throws SQLException {
        if (conn != null) {
            stmt5.setInt(1, cnt);
            stmt5.setString(2, product_code);
            stmt5.setString(3, product_name);
            stmt5.addBatch();
        }
    }

    private static void loadOrders(int cnt, String product_code, int quantity, Date estimated_delivery_date, Date lodgement_date, String contract_number, int salesman_number) throws SQLException {
        if (conn != null) {
            stmt6.setInt(1, cnt);
            stmt6.setString(2, product_code);
            stmt6.setInt(3, quantity);
            stmt6.setDate(4, estimated_delivery_date);
            stmt6.setDate(5, lodgement_date);
            stmt6.setString(6, contract_number);
            stmt6.setInt(7, salesman_number);
            stmt6.addBatch();
        }
    }

    private static void loadModel(int cnt, String product_model, int unit_price, String product_code) throws SQLException {
        if (conn != null) {
            stmt7.setInt(1, cnt);
            stmt7.setString(2, product_model);
            stmt7.setInt(3, unit_price);
            stmt7.setString(4, product_code);
            stmt7.addBatch();
        }
    }

    private static void loadOrdersx(int cnt, String product_code, int quantity, Date estimated_delivery_date, Date lodgement_date, String contract_number, int salesman_number) throws SQLException {
        if (conn != null) {
            stmtx.setInt(1, cnt);
            stmtx.setString(2, product_code);
            stmtx.setInt(3, quantity);
            stmtx.setDate(4, estimated_delivery_date);
            stmtx.setDate(5, lodgement_date);
            stmtx.setString(6, contract_number);
            stmtx.setInt(7, salesman_number);
            stmtx.addBatch();
        }
    }
}
