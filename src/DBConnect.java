import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DBConnect {
    int num;
    int cnt;
    long start;
    long stop;
    Connection conn;
    PreparedStatement stmt;
    BufferedReader infile;
    String line;
    String[] parts;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final String source_dir = "/Users/blank/IdeaProjects/DB_project_1/src/splits";


    public DBConnect(){

    }

    public DBConnect(int num, int cnt, Connection conn, PreparedStatement stmt){
        this.num = num;
        this.cnt = cnt;
        this.conn = conn;
        this.stmt = stmt;
    }

    public long connect(int count) throws IOException, SQLException, ParseException {
        int totalNum = 0;
        start = System.currentTimeMillis();
        switch (num){
            case 3:
                infile = new BufferedReader(new FileReader(source_dir + "/contract_sub.csv"));
                infile.skip(57);
                String contract_number;
                String director;
                Date contract_date;
                String client_enterprise;
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
                totalNum += (cnt-200000);
                stmt.executeBatch();
                stmt.close();
                break;
            case 5:
                infile = new BufferedReader(new FileReader(source_dir + "/product_sub.csv"));
                infile.skip(26);
                String product_code;
                String product_name;
                while((line = infile.readLine()) != null){
                    parts = line.split(",");
                    if(parts.length > 1){
                        product_code = parts[0];
                        product_name = parts[1];
                        loadProduct(cnt, product_code, product_name);
                    }
                    cnt++;
                }
                totalNum += (cnt-400000);
                stmt.executeBatch();
                stmt.close();
                break;
            case 6:
                infile = new BufferedReader(new FileReader(source_dir + "/orders_sub.csv"));
                infile.skip(93);
                int quantity;
                Date estimated_delivery_date;
                Date lodgement_date;
                int salesman_number;
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
                            if(lodgement_date.after(simpleDateFormat.parse("2022-03-02"))){
                                lodgement_date = null;
                            }
                        }else{
                            lodgement_date = null;
                        }
                        contract_number = parts[4];
                        salesman_number = Integer.parseInt(parts[5]);
                        loadOrders(cnt, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number);
                    }
                    cnt++;
                }
                totalNum += (cnt-500000);
                stmt.executeBatch();
                stmt.close();
                break;
            case 7:
                infile = new BufferedReader(new FileReader(source_dir + "/model_sub.csv"));
                infile.skip(34);
                String product_model;
                int unit_price;
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
                totalNum += (cnt-600000);
                stmt.executeBatch();
                stmt.close();
                break;
        }
        count += totalNum;
        stop = System.currentTimeMillis();
        return stop-start;
    }

    public long connect(Map<String, Integer> lookUp, int count) throws IOException, SQLException {
        int totalNum = 0;
        start = System.currentTimeMillis();
        switch (num){
            case 1:
                infile = new BufferedReader(new FileReader(source_dir + "/center_sub.csv"));
                infile.skip(23);
                String supply_center;
                String industry;
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
                totalNum += cnt;
                stmt.executeBatch();
                stmt.close();
                break;
            case 2:
                infile = new BufferedReader(new FileReader(source_dir + "/enterprise_sub.csv"));
                infile.skip(54);
                String client_enterprise;
                String country;
                String city;
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
                totalNum += (cnt-100000);
                stmt.executeBatch();
                stmt.close();
                break;
            case 4:
                infile = new BufferedReader(new FileReader(source_dir + "/salesman_sub.csv"));
                infile.skip(72);
                int salesman_number;
                String salesman;
                String gender;
                int age;
                long mobile_phone;
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
                totalNum += (cnt-300000);
                stmt.executeBatch();
                stmt.close();
                break;

        }
        count += totalNum;
        stop = System.currentTimeMillis();
        return stop-start;
    }

    private void loadCenter(int cnt, String supply_center, String industry) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setString(2, supply_center);
            stmt.setString(3, industry);
            stmt.addBatch();
        }
    }

    private void loadEnterprise(int cnt, String client_enterprise, String country, String city, int id) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setString(2, client_enterprise);
            stmt.setString(3, country);
            stmt.setString(4, city);
            stmt.setInt(5, id);
            stmt.addBatch();
        }
    }

    private void loadContract(int cnt, String contract_number, String director, Date contract_date, String client_enterprise) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setString(2, contract_number);
            stmt.setString(3, director);
            stmt.setDate(4, contract_date);
            stmt.setString(5, client_enterprise);
            stmt.addBatch();
        }
    }

    private void loadSale_man(int cnt, int salesman_number, String salesman, String gender, int age, long mobile_phone, int id) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setInt(2, salesman_number);
            stmt.setString(3, salesman);
            stmt.setString(4, gender);
            stmt.setInt(5, age);
            stmt.setLong(6, mobile_phone);
            stmt.setInt(7, id);
            stmt.addBatch();
        }
    }

    private void loadProduct(int cnt, String product_code, String product_name) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setString(2, product_code);
            stmt.setString(3, product_name);
            stmt.addBatch();
        }
    }

    private void loadOrders(int cnt, String product_code, int quantity, Date estimated_delivery_date, Date lodgement_date, String contract_number, int salesman_number) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setString(2, product_code);
            stmt.setInt(3, quantity);
            stmt.setDate(4, estimated_delivery_date);
            stmt.setDate(5, lodgement_date);
            stmt.setString(6, contract_number);
            stmt.setInt(7, salesman_number);
            stmt.addBatch();
        }
    }

    private void loadModel(int cnt, String product_model, int unit_price, String product_code) throws SQLException {
        if (conn != null) {
            stmt.setInt(1, cnt);
            stmt.setString(2, product_model);
            stmt.setInt(3, unit_price);
            stmt.setString(4, product_code);
            stmt.addBatch();
        }
    }
}
