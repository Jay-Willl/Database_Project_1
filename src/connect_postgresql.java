import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

public class connect_postgresql {
    static Object lock1 = new Object();
    static Object lock2 = new Object();
    static Object lock3 = new Object();
    static Object lock4 = new Object();
    static Object lock5 = new Object();
    static Object lock6 = new Object();
    static Object lock7 = new Object();
    static Object lock8 = new Object();
    static volatile Boolean t1 = false; // center !important
    static volatile Boolean t2 = false;
    static volatile Boolean t3 = false;
    static volatile Boolean t4 = false;
    static volatile Boolean t5 = false; // product
    static volatile Boolean t6 = false;
    static volatile Boolean t7 = false; // model !important
    public connect_postgresql(){}

    public long create_connect() throws SQLException {
        long start = System.currentTimeMillis();
        top.conn = DriverManager.getConnection(top.url,top.username,top.password);
        top.conn.setAutoCommit(false);

        top.stmt1 = top.conn.prepareStatement(
                "insert into center(center_id, supply_center, industry)"
                        + " values(?,?,?)");
        top.stmt2 = top.conn.prepareStatement(
                "insert into enterprise(enterprise_id, client_enterprise, country, city, center_id)"
                        + " values(?,?,?,?,?)");
        top.stmt3 = top.conn.prepareStatement(
                "insert into contract(contract_id, contract_number, director, contract_date, client_enterprise)"
                        + " values(?,?,?,?,?)");
        top.stmt4 = top.conn.prepareStatement(
                "insert into sale_man(salesman_id, salesman_number, salesman, gender, age, mobile_phone,center_id)"
                        + " values(?,?,?,?,?,?,?)");
        top.stmt5 = top.conn.prepareStatement(
                "insert into product(product_id, product_code, product_name)"
                        + " values(?,?,?)");
        top.stmt6 = top.conn.prepareStatement(
                "insert into orders(orders_id, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number)"
                        + " values(?,?,?,?,?,?,?)");
        top.stmt7 = top.conn.prepareStatement(
                "insert into model(model_id, product_model, unit_price, product_code)"
                        + " values(?,?,?,?)");
        long stop = System.currentTimeMillis();
        return stop - start;
    }

    public long create_connect_my() throws SQLException {
        long start = System.currentTimeMillis();
        top.conn = DriverManager.getConnection(top.url_my,top.username_my,top.password_my);
        top.conn.setAutoCommit(false);

        top.stmt1 = top.conn.prepareStatement(
                "insert into center(center_id, supply_center, industry)"
                        + " values(?,?,?)");
        top.stmt2 = top.conn.prepareStatement(
                "insert into enterprise(enterprise_id, client_enterprise, country, city, center_id)"
                        + " values(?,?,?,?,?)");
        top.stmt3 = top.conn.prepareStatement(
                "insert into contract(contract_id, contract_number, director, contract_date, client_enterprise)"
                        + " values(?,?,?,?,?)");
        top.stmt4 = top.conn.prepareStatement(
                "insert into sale_man(salesman_id, salesman_number, salesman, gender, age, mobile_phone,center_id)"
                        + " values(?,?,?,?,?,?,?)");
        top.stmt5 = top.conn.prepareStatement(
                "insert into product(product_id, product_code, product_name)"
                        + " values(?,?,?)");
        top.stmt6 = top.conn.prepareStatement(
                "insert into orders(orders_id, product_code, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_number)"
                        + " values(?,?,?,?,?,?,?)");
        top.stmt7 = top.conn.prepareStatement(
                "insert into model(model_id, product_model, unit_price, product_code)"
                        + " values(?,?,?,?)");
        long stop = System.currentTimeMillis();
        return stop - start;
    }
    public long pg_import() throws SQLException, IOException, InterruptedException {
        long start = System.currentTimeMillis();
        int count = 0;
        Map<String, Integer> lookUp = new HashMap<>();

        class Run1 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run1(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock1) {
                    DBConnect dbConnect = new DBConnect(1, cnt, conn, stmt);
                    try {
                        long time = dbConnect.connect(lookUp, count);
                        t1 = true;
                        lock1.notify();
                        System.out.println("        table center has been imported, time: "+time+" ms");
                    } catch (IOException | SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        class Run2 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run2(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock1) {
                    synchronized (lock2) {
                        DBConnect dbConnect = new DBConnect(2, cnt, conn, stmt);
                        try {
                            if(!t1){
                                lock1.wait();
                            }
                            long time = dbConnect.connect(lookUp, count);
                            t2 = true;
                            lock2.notify();
                            System.out.println("        table enterprise has been imported, time: "+time+" ms");
                        } catch (IOException | SQLException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        class Run4 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run4(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock1) {
                    synchronized (lock4){
                        DBConnect dbConnect = new DBConnect(4, cnt, conn, stmt);
                        try {
                            if(!t1){
                                lock1.wait();
                            }
                            long time = dbConnect.connect(lookUp, count);
                            t4 = true;
                            lock4.notify();
                            System.out.println("        table salesman has been imported, time: "+time+" ms");
                        } catch (IOException | SQLException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        class Run3 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run3(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock2) {
                    synchronized (lock3) {
                        DBConnect dbConnect = new DBConnect(3, cnt, conn, stmt);
                        try {
                            if(!t2){
                                lock2.wait();
                            }
                            long time = dbConnect.connect(count);
                            t3 = true;
                            lock3.notify();
                            System.out.println("        table contract has been imported, time: "+time+" ms");
                        } catch (IOException | SQLException | InterruptedException | ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }
            }
        }

        class Run5 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run5(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock5) {
                    DBConnect dbConnect = new DBConnect(5, cnt, conn, stmt);
                    try {
                        long time = dbConnect.connect(count);
                        t5 = true;
                        lock5.notify();
                        System.out.println("        table product has been imported, time: "+time+" ms");
                    } catch (IOException | SQLException | ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        class Run6 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run6(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock3) {
                    synchronized (lock4){
                        synchronized (lock5){
                            DBConnect dbConnect = new DBConnect(6, cnt, conn, stmt);
                            try {
                                if(!t3 || !t4 || !t5){
                                    lock3.wait();
                                    lock4.wait();
                                    lock5.wait();
                                }
                                long time = dbConnect.connect(count);
                                System.out.println("        table orders has been imported, time: "+time+" ms");
                            } catch (IOException | SQLException | InterruptedException | ParseException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                }
            }
        }

        class Run7 implements Runnable{
            DBConnect dbConnect;
            long time;
            final int num;
            final int cnt;
            final Connection conn;
            final PreparedStatement stmt;
            public Run7(int num, int cnt, Connection conn, PreparedStatement stmt){
                this.num = num;
                this.cnt = cnt;
                this.conn = conn;
                this.stmt = stmt;
            }
            @Override
            public void run() {
                synchronized (lock5) {
                    DBConnect dbConnect = new DBConnect(7, cnt, conn, stmt);
                    try {
                        if(!t5){
                            lock5.wait();
                        }
                        long time = dbConnect.connect(count);
                        System.out.println("        table model has been imported, time: "+time+" ms");
                    } catch (IOException | SQLException | InterruptedException | ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        Thread thread1 = new Thread(new Run1(1, 1, top.conn, top.stmt1)); // center
        Thread thread2 = new Thread(new Run2(2, 100001, top.conn, top.stmt2)); // enterprise
        Thread thread3 = new Thread(new Run3(3, 200001, top.conn, top.stmt3)); // contract
        Thread thread4 = new Thread(new Run4(4, 300001, top.conn, top.stmt4)); // salesman
        Thread thread5 = new Thread(new Run5(5, 400001, top.conn, top.stmt5)); // product
        Thread thread6 = new Thread(new Run6(6, 500001, top.conn, top.stmt6)); // orders
        Thread thread7 = new Thread(new Run7(7, 600001, top.conn, top.stmt7)); // model

        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        thread5.start();
        thread6.start();
        thread7.start();
        thread6.join();

//        {@Deprecated
//        ThreadPoolExecutor threadPoolExecutor1 = new ThreadPoolExecutor(4, 5, 500, TimeUnit.MILLISECONDS,
//                new ArrayBlockingQueue<Runnable>(5));
//        ThreadPoolExecutor threadPoolExecutor2 = new ThreadPoolExecutor(2, 3, 500, TimeUnit.MILLISECONDS,
//                new ArrayBlockingQueue<Runnable>(3));
//        threadPoolExecutor1.getQueue().add(thread1);
//        threadPoolExecutor1.getQueue().add(thread2);
//        threadPoolExecutor1.getQueue().add(thread3);
//        threadPoolExecutor1.getQueue().add(thread4);
//        threadPoolExecutor2.getQueue().add(thread5);
//        threadPoolExecutor2.getQueue().add(thread7);
//
//        threadPoolExecutor1.execute(new Runnable() {
//            @Override
//            public void run() {
//                while(!threadPoolExecutor1.getQueue().isEmpty()){
//                    threadPoolExecutor1.getQueue().poll().run();
//                }
//            }
//        });
//        threadPoolExecutor2.execute(new Runnable() {
//            @Override
//            public void run() {
//                while(!threadPoolExecutor1.getQueue().isEmpty()){
//                    threadPoolExecutor1.getQueue().poll().run();
//                }
//            }
//        });
//
//        if(threadPoolExecutor1.isTerminated()){
//            thread6.start();
//            thread4.start();
//        }
//        Thread.sleep(1000);}

        top.conn.commit();
        top.conn.close();
        long stop = System.currentTimeMillis();
        return stop-start;
    }
}