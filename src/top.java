import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
public class top {
    static Connection conn = null;
    static PreparedStatement  stmt1 = null;
    static PreparedStatement  stmt2 = null;
    static PreparedStatement  stmt3 = null;
    static PreparedStatement  stmt4 = null;
    static PreparedStatement  stmt5 = null;
    static PreparedStatement  stmt6 = null;
    static PreparedStatement  stmt7 = null;
    static connect_postgresql cp;
    static connect_postgresql my;
    static String url_my = "jdbc:mysql://localhost:3306/project_1";
    static String username_my = "";
    static String password_my = "";
    static int count = 0;
    static Map<String, Integer> lookUp = new HashMap<>();
    static Object lock1 = new Object();
    static Object lock2 = new Object();
    static volatile boolean t1 = false;
    static volatile boolean t2 = false;
    static String url = "jdbc:postgresql://localhost:5432/project_1";
    static String username = "blank";
    static String password = "411128";
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        System.out.println("currently running: (test computer 1, PostgreSQL)");
        long start = System.currentTimeMillis();
        class Pre implements Runnable {
            @Override
            public void run() {
                synchronized (lock1){
                    System.out.println("    Pretreatment time: "+pretreatment()+" ms");
                    t1 = true;
                    lock1.notify();
                }
            }
        }

        class Con implements Runnable {
            @Override
            public void run() {
                synchronized (lock2) {
                    try {
                        top.cp = new connect_postgresql();
                        System.out.println("    Connect time: "+cp.create_connect()+" ms");
                        t2 = true;
                        lock2.notify();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }
        class Imp implements Runnable {
            @Override
            public void run() {
                synchronized (lock1) {
                    try {
                        if(!t1){
                            lock1.wait();
                        }
                        System.out.println("    Import time: "+cp.pg_import()+" ms");
                    } catch (SQLException | IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        Thread thread1 = new Thread(new Pre());
        Thread thread2 = new Thread(new Con());
        Thread thread3 = new Thread(new Imp());
        thread1.start();
        thread2.start();
        thread3.start();
        thread3.join();
        long stop = System.currentTimeMillis();
        System.out.println("Total time: "+(stop-start)+" ms");

        System.out.println("currently running: (test computer 1, MySQL)");
        connect_mysql.exe();

    }

    public static long pretreatment(){
        long start;
        long stop;
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
        stop = System.currentTimeMillis();
        return stop-start;
    }
}
