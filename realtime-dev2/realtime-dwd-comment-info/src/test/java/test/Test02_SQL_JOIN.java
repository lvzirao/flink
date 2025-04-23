package test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Package com.lzr.realtime.dwd.test.Test02_SQL_JOIN
 * @Author lv.zirao
 * @Date 2025/4/15 11:16
 * @description:
 * 该案例演示了通过FlinkSQL双流join
 *                  左表                  右表
 * 内连接      OnCreateAndWrite      OnCreateAndWrite
 * 左外连接    OnReadAndWrite        OnCreateAndWrite
 * 右外连接    OnCreateAndWrite      OnReadAndWrite
 * 全外连接    OnReadAndWrite        OnReadAndWrite
 */
public class Test02_SQL_JOIN {
    public static void main(String[] args) {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(1);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 设置状态的保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 2.检查点相关的设置(略)
        // TODO 3.从指定的网络端口读取员工数据 并转换为动态表
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("cdh01", 8888)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String lineStr) throws Exception {
                        String[] fieldArr = lineStr.split(",");
                        return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                    }
                });
        tableEnv.createTemporaryView("emp",empDS);
        // TODO 4.从指定的网络端口读取部门数据 并转换为动态表
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("cdh01", 8889)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String lineStr) throws Exception {
                        String[] fieldArr = lineStr.split(",");
                        return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                    }
                });
        tableEnv.createTemporaryView("dept",deptDS);
        // TODO 5.内连接
        // 注意：如果使用普通的内外连接，底层会为参与连接的两张表各自维护一个状态，用于存放两张表的数据，默认情况下，状态用不会失效
        // 在生产环境中，一定要设置状态的保留时间
        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e join dept d on e.deptno=d.deptno").print();
        // TODO 6.左外连接
        // 注意：如果左表数据先到，右表数据后到，会产生3条数据
        // 左表  null  标记为+I
        // 左表  null  标记为-D
        // 左表  右表  标记为+I
        // 这样的动态转换的流称之为回撤流
        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno=d.deptno").print();
        // TODO 7.右外连接
        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e right join dept d on e.deptno=d.deptno").print();
        // TODO 8.全外连接
        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e full join dept d on e.deptno=d.deptno").print();
        // TODO 9.将左外连接的结果写道kafka表
        // 注意：kafka连接器不支持写入的时候包含update或者delete操作，需要用upsert-kafka连接器
        // 如果左外连接，左表数据先到，右表数据后到，会有3条结果产生
        // 左表  null  标记为+I
        // 左表  null  标记为-D
        // 左表  右表  标记为+I
        // 这样的数据如果写道kafka主题中，kafka主题会接收到3条消息
        // 左表 null
        // null
        // 左表 右表
        // 如果从kafka主题中读取数据的时候，存在空消息，如果使用的FlinkSQL的方式读取，会自动的将消息过滤掉：如果使用的是FlinkAPI的方式读取的话，默认的SimpleStringSchema是处理不了
        // 空消息的，需要自定义反序列化器
        // 出了空消息外，在DWS层进行汇总操作的时候，还需要进行去重处理
        // 9.1 创建一个动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("create table emp_dept (\n"+
                " empno integer,\n" +
                " ename string,\n" +
                " deptno integer,\n" +
                " dname string,\n" +
                " PRIMARY KEY (empno) NOT ENFORCED\n"+
                ") with (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'first',\n" +
                " 'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                " 'format' = 'json'\n" +
                ")");
        // 9.2 写入
        tableEnv.executeSql("insert into emp_dept select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno=d.deptno");



    }








































}
