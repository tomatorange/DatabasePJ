package code;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;


class ExecuteDDL {

    private String driver;

    private String url;

    private String user;

    private String pass;

    Connection conn;

    Statement stmt;

    public void initParam(String paramFile) throws Exception {

        Properties props = new Properties();

        props.load(new FileInputStream(paramFile));

        driver = props.getProperty("driver");

        url = props.getProperty("url");

        user = props.getProperty("user");

        pass = props.getProperty("pass");

    }


    public void createTable(String csvFileName,String tableName,String code) throws Exception{

        try {

            Class.forName(driver);
            conn = DriverManager.getConnection(url,user,pass);
            stmt = conn.createStatement();
            String sql;

            DatabaseMetaData metaData = conn.getMetaData();
            String[] types = {"TABLE"};
            ResultSet rs = metaData.getTables(null, null, "%", types);
            while (rs.next()) {
                String tableCatalog = rs.getString(1);
                String tableSchema = rs.getString(2);
                String oldtableName = rs.getString(3);
                if(oldtableName.equals(tableName))
                {
                    sql="drop table "+tableName;
                    System.out.println(sql);
                    stmt.executeUpdate(sql);
                }
            }


            InputStreamReader isr=new InputStreamReader(new FileInputStream(csvFileName),code);
            try (Scanner sc = new Scanner(isr)) {

                //读入第一行并根据第一行内容建表
                String first_line= sc.nextLine();
                //System.out.println(first_line);
                String[] split1 = first_line.split(",");
                sql="create table "+tableName+" (";
                for (int i=0;i< split1.length;i++) {
                    char c[]=split1[i].toCharArray();
                    String s="";
                    if(c[0]=='"')
                    {
                        for (int j = 1; j < c.length-1; j++) {
                            s+=c[j];
                        }
                    }
                    else
                        s=split1[i];
                    sql+=s;
                    sql+=" varchar(50)";
                    if(i!=split1.length-1)
                        sql+=",";
                }
                sql+=");";
                //System.out.println(sql);
                stmt.executeUpdate(sql);

                //逐行插入
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    //System.out.println(line);
                    String[] splits = line.split(",");
                    sql="insert into "+tableName+" values( ";
                    for (int i=0;i< split1.length;i++) {
                        if(i<splits.length)
                        {
                            char c[]=splits[i].toCharArray();
                            String s="";
                            if(c[0]=='"')
                            {
                                for (int j = 1; j < c.length-1; j++) {
                                    s+=c[j];
                                }
                            }
                            else
                                s=splits[i];
                            splits[i]="'"+s+"'";
                            sql+=splits[i];
                        }
                        else
                            sql+="NULL";
                        if(i!=split1.length-1)
                            sql+=",";
                    }
                    sql+=");";
                    //System.out.println(sql);
                    stmt.executeUpdate(sql);
                }
            }
        }
        finally
        {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String tableName;
        String csvFileName;
        String initFileName;
        String code;

        //D:\Course\Database\Lab1\part1\src\csv\student.csv
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入配置文件名：");
        initFileName= sc.nextLine();
        System.out.println("请输入csv文件名：");
        csvFileName= sc.nextLine();
        System.out.println("请输入表名：");
        tableName= sc.nextLine();
        System.out.println("请输入文件编码格式：如GBK,UTF-8");
        code= sc.nextLine();
        ExecuteDDL ed = new ExecuteDDL();
        //D:\Course\Database\Lab1\part1\src\connext.properties
        ed.initParam(initFileName);
        ed.createTable(csvFileName,tableName,code);
        System.out.println("Execute DDL success");
    }

}
