package org.saiku.service.util.export;

import org.apache.commons.lang.StringUtils;
import org.olap4j.CellSet;
import org.saiku.olap.dto.resultset.AbstractBaseCell;
import org.saiku.olap.dto.resultset.CellDataSet;
import org.saiku.olap.dto.resultset.DataCell;
import org.saiku.olap.dto.resultset.MemberCell;
import org.saiku.olap.util.OlapResultSetUtil;
import org.saiku.olap.util.SaikuProperties;
import org.saiku.olap.util.formatter.CellSetFormatter;
import org.saiku.olap.util.formatter.ICellSetFormatter;
import org.saiku.service.util.KeyValue;
import org.saiku.service.util.exception.SaikuServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

public class DBExporter {
	private static final Logger log = LoggerFactory.getLogger(DBExporter.class);
    private static Properties properties = null;
    
	private static String DRIVER = null;  
    private static String url = null;
    private static String user= null;
    private static String password = null;
    private static String database = null;
    private static String useUnicode = "useUnicode=true&characterEncoding=UTF8";
  
    static {
    	properties=new Properties();
        try {
			properties.load(DBExporter.class.getClassLoader().getResourceAsStream("/connection.properties"));
			DRIVER = properties.getProperty("driver").trim();
			url = properties.getProperty("location").trim();
			database = properties.getProperty("database").trim();
			url = url + database +"?"+ useUnicode;
			user = properties.getProperty("username").trim();
			password = properties.getProperty("password").trim();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("读取配置文件出错");
			e.printStackTrace();
		}
    } 
    
    private static String exportDB(CellSet cellSet) {
	    return exportDB( cellSet,  new CellSetFormatter() ,"");
	}
    
    public static String exportDB(CellSet cellSet, ICellSetFormatter formatter ,String tableName) {
		List<CellSetAxis> axes = cellSet.getAxes();
		CellSetAxis cAxis = axes.get(Axis.COLUMNS.axisOrdinal());
		CellSetAxis rAxis = axes.get(Axis.ROWS.axisOrdinal());
		List<Position> cps = cAxis.getPositions();
		List<Position> rps = rAxis.getPositions();

		if (rps.size() < 1 || cps.size() < 1)
			return "Failed";

		ArrayList<Member> memberlList = new ArrayList<Member>();
		for (Member m : rps.get(0).getMembers()) {
			memberlList.add(m);
		}
		for (Member m : cps.get(0).getMembers()) {
			memberlList.add(m);
		}
		//存储每个最大深度的Member
		Member[] marray = memberlList.toArray(new Member[memberlList.size()]);
		for (Position rposition : rps) {
			List<Member> rmemberlist = rposition.getMembers();
			for (int i = 0; i < rmemberlist.size(); i++) {
				if (marray[i].getDepth() < rmemberlist.get(i).getDepth()) {
					marray[i] = rmemberlist.get(i);
				}
			}
		}
		for (Position cposition : cps) {

			List<Member> rmemberlist = cposition.getMembers();
			for (int i = 0; i < rmemberlist.size(); i++) {
				if (marray[i+rps.get(0).getMembers().size()].getDepth() < rmemberlist.get(i).getDepth()) {
					marray[i+rps.get(0).getMembers().size()] = rmemberlist.get(i);
				}
			}
		}
		//表头字段
		ArrayList<String> tableHeader=new ArrayList<>();
		String str1 = "";
		for (int i = 0; i < marray.length; i++) {
			Member m = marray[i];
			Member temp = m;
			
			if (m.getDepth() == 0) {
				str1 = m.getDimension().getName() + "\t" + str1;
				tableHeader.add(m.getDimension().getName());
			} else {
				for (int j = m.getDepth(); j > 0; j--) {
					tableHeader.add(temp.getLevel().getName());
					str1 = temp.getLevel().getName() + "\t" + str1;
					temp = temp.getParentMember();
				}
			}
		}
		tableHeader.add("value");
//		System.out.println(str1);
		
		ArrayList<ArrayList<Object>>tableBody=new ArrayList<>();
		//表的数据内容与表字段顺序一一对应
		for (Position rposition : rps) {
			for (Position cposition : cps) {
				ArrayList<Object> tablerow=new ArrayList<>();
				boolean isnessar = true;
				List<Member> rmemberlist = rposition.getMembers();
				List<Member> cmemberlist = cposition.getMembers();
				String str = "";
				for (int i = 0; i < rmemberlist.size(); i++) {
					Member m = rmemberlist.get(i);
					if (m.getDepth() == marray[i].getDepth()) {

						Member temp = m;
						if (m.getDepth() == 0) {
							str = m.getName() + "\t" + str;
							tablerow.add(m.getName());
						} else {
							for (int j = m.getDepth(); j > 0; j--) {
								tablerow.add(temp.getName());
								str = temp.getName() + "\t" + str;
								temp = temp.getParentMember();
							}
						}

					} else {
						isnessar = false;
						break;
					}

				}
				if (!isnessar)
					continue;
				for (int i = 0; i < cmemberlist.size(); i++) {
					Member m = cmemberlist.get(i);
					if (m.getDepth() == marray[i + rmemberlist.size()]
							.getDepth()) {

						Member temp = m;
						if (m.getDepth() == 0) {
							tablerow.add(m.getName());
							str = m.getName() + "\t" + str;
						} else {
							for (int j = m.getDepth(); j > 0; j--) {
								tablerow.add(temp.getName());
								str = temp.getName() + "\t" + str;
								temp = temp.getParentMember();
							}
						}

					} else {
						isnessar = false;
						break;
					}
				}
				if (!isnessar)
					continue;

				Cell cell = cellSet.getCell(cposition, rposition);
				String value = cell.getFormattedValue();
				if(StringUtils.isNotBlank(value)){
					tablerow.add(value);
					tableBody.add(tablerow);
				}
//				System.out.println(str + value);
			}
		}
		if(tableName==""){
			try {
				tableName = cellSet.getMetaData().getCube().getName()+new Date().getTime();
			} catch (OlapException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				log.error("OlapException", e);
			}
		}
		
		return	saveToDB(tableName,tableHeader, tableBody);
		
	}
	
	private static String saveToDB(String tableName,ArrayList<String> tableHeader,ArrayList<ArrayList<Object>>tableBody){
//		for(String s:tableHeader){
//			System.out.print(s + "\t");
//		}
//		System.out.println();
//		for(ArrayList<Object> a:tableBody){
//			for(Object s:a){
//				System.out.print(s + "\t");
//			}
//			System.out.println();
//		}
		
		if(StringUtils.isBlank(tableName))return "表名不能为空";
		Connection conn = null;
        String sql;
        // MySQL的JDBC URL编写方式：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
        // 避免中文乱码要指定useUnicode和characterEncoding
        // 执行数据库操作之前要在数据库管理系统上创建一个数据库，名字自己定，
        // 下面语句之前就要先创建javademo数据库
        
 
        try {
            // 之所以要使用下面这条语句，是因为要使用MySQL的驱动，所以我们要把它驱动起来，
            // 可以通过Class.forName把它加载进去，也可以通过初始化来驱动起来，下面三种形式都可以
            Class.forName(DRIVER);// 动态加载mysql驱动
            // or:
            // com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
            // or：
            // new com.mysql.jdbc.Driver();
 
            System.out.println("成功加载MySQL驱动程序");
            // 一个Connection代表一个数据库连接
            conn = DriverManager.getConnection(url,user,password);
            ResultSet rs=conn.getMetaData().getTables(null,null,tableName,null);
            if(rs.next()){
            	return "表已存在！";
            }
            // Statement里面带有很多方法，比如executeUpdate可以实现插入，更新和删除等
            Statement stmt = conn.createStatement();
//            sql = "drop table if exists "+tableName ;
//            stmt.executeUpdate(sql);
            sql = "create table "+tableName+"(NO int(11) NOT NULL AUTO_INCREMENT,";
            for(String s:tableHeader){
            	sql+=s+" varchar(255),";
            	
            }
            sql+="primary key(NO))";
            int result = stmt.executeUpdate(sql);// executeUpdate语句会返回一个受影响的行数，如果返回-1就没有成功
            if (result != -1) {
                System.out.println("创建数据表成功");
                
                String temp="insert into "+tableName+"(NO";
                for(String s:tableHeader){
                	temp+=","+s;
                }
                temp+=") values(null";
                for(int i=0;i<tableHeader.size();i++){
                	temp+=",?";
                }
                temp+=")";
                
                PreparedStatement ps = conn.prepareStatement(temp);
                conn.setAutoCommit(false);
                for(ArrayList<Object> a:tableBody){
                	for(int i=0;i<a.size();i++){
                    	ps.setString(i+1, a.get(i).toString());
                    }
                	ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);
//                sql = "insert into student(NO,name) values('2012001','陶伟基')";
//                result = stmt.executeUpdate(sql);
//                sql = "insert into student(NO,name) values('2012002','周小俊')";
//                result = stmt.executeUpdate(sql);
               
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("MySQL操作错误");
            //e.printStackTrace();
            log.error("SQLException | ClassNotFoundException", e);
        } catch (Exception e){
        	//if(!conn.isClosed()) conn.close();
        	//e.printStackTrace();
        	log.error("Exception", e);
        	return e.getMessage();
        }finally {
            try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.error("SQLException", e);
				//e.printStackTrace();
				return e.getMessage();
			}
        }
        return "Success";
	}
}
