package com.lzr.realtime.util;

import com.google.common.base.CaseFormat;
import com.lzr.realtime.constant.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.lzr.realtime.util.JdbcUtil
 * @Author lv.zirao
 * @Date 2025/4/13 19:52
 * @description:
 * 通过jdbc操作mysql数据库
 */
public class JdbcUtil {
    // 获取mysql连接
    public static Connection getMySQLConnection() throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        return conn;
    }
    // 关闭mysql数据库
    public static void closeMySQLConnection(Connection conn)throws SQLException{
        if (conn!=null && !conn.isClosed()){
            conn.close();
        }
    }
    // 从数据库表中查询数据
   public static <T> List<T> querList(Connection coon, String sql,Class<T> clz,boolean... isUnderlineToCamel) throws Exception{
       List<T> resList = new ArrayList<>();
       boolean defaultIsUToC=false;  // 默认不执行下划线转驼峰

       if (isUnderlineToCamel.length>0){
           defaultIsUToC=isUnderlineToCamel[0];
       }

       PreparedStatement ps = coon.prepareStatement(sql);
       ResultSet rs = ps.executeQuery();
       ResultSetMetaData metaData = rs.getMetaData();
       while (rs.next()){
           // 通过反射创建一个对象，用于接收查询结果
           T obj = clz.newInstance();
           for (int i=1; i<=metaData.getColumnCount();i++){
               String columName = metaData.getColumnClassName(i);
               Object columValue = rs.getObject(i);
               // 给对象的属性赋值
               if(defaultIsUToC){
                   columName= CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columName);
               }
               BeanUtils.setProperty(obj,columName,columValue);
           }
           resList.add(obj);

       }
       return resList;
   }
}
