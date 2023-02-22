package com.dongchanglong.shardingdemo;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.InlineShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class TestSharding {
    //pool
    public static DataSource createDataSource(String user, String password, String url) {
        DruidDataSource ds = new DruidDataSource();
        ds.setUsername(user);
        ds.setPassword(password);
        ds.setUrl(url);
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        return ds;
    }

    public static void main(String[] args) {

        //配置数据源
        Map<String, DataSource> map = new HashMap();
        map.put("test0", createDataSource("root", "1111", "jdbc:mysql://127.0.0.1:3306/test0?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false"));
        map.put("test1", createDataSource("root", "1111", "jdbc:mysql://127.0.0.1:3306/test1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false"));

        ShardingRuleConfiguration config = new ShardingRuleConfiguration();

        //配置数据节点
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration("t_order", "test${0..1}.t_order_${0..1}");

        //配置主键生成策略
        KeyGeneratorConfiguration key = new KeyGeneratorConfiguration("SNOWFLAKE", "oid");
        orderTableRuleConfig.setKeyGeneratorConfig(key);

        //配置分库策略
        orderTableRuleConfig.setDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("uid", "test${uid % 2}"));

        //配置分表策略
        orderTableRuleConfig.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("oid", "t_order_${oid % 2}"));

        config.getTableRuleConfigs().add(orderTableRuleConfig);

        config.getBroadcastTables().add("t_order_$->{0..1}");
        try {
            //获取数据源
            DataSource ds = ShardingDataSourceFactory.createDataSource(map, config, new Properties());

//            //插入
//            for (int i = 1; i <= 10; ++i) {
//                String sql = "insert into t_order(uid,name) values(?,?)";
//                execute(ds, sql, i, i + "aaa");
//            }

            //查询
            String sql2 = "select * from t_order order by oid";
            executeQuery(ds, sql2);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }



    //插入
    public static void execute(DataSource ds, String sql, int uid, String name) throws Exception {
        Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1, uid);
        ps.setString(2, name);
        ps.execute();
    }

    //查询
    public static void executeQuery(DataSource ds, String sql) throws Exception {
        Connection conn = ds.getConnection();
        Statement stat = conn.createStatement();
        ResultSet result = stat.executeQuery(sql);
        while (result.next()) {
            System.out.println(result.getLong(1) + "\t|\t" + result.getInt(2)
                    + "\t|\t" + result.getString(3));
            System.out.println("----------------------------------");
        }
        result.close();
        stat.close();
    }
}