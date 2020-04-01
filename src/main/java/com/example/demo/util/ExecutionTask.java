package com.example.demo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@RestController
public class ExecutionTask implements ApplicationRunner {

        @Autowired
        private KafkaManager kafkaManager;

    String destination;

//    static long aLong = 0 ;

    public ExecutionTask(){
        this.destination = GlobalConfigUtil.canalInstance;
    }

    public static CanalConnector getConn(String host, int port, String instance, String username, String password) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), instance, username, password);
        return canalConnector;
    }

    @Override
    public void run(ApplicationArguments args){

        System.out.println("执行了.......");
//        //单机模式
//        String host = GlobalConfigUtil.canalHost;
//        int port = Integer.parseInt(GlobalConfigUtil.canalPort);
//        String instance = GlobalConfigUtil.canalInstance;
//        String username = GlobalConfigUtil.mysqlUsername;
//        String password = GlobalConfigUtil.mysqlPassword;
//        // 获取Canal连接
//        CanalConnector conn = getConn(host, port, instance, username, password);

        //集群模式
        String zookeeper = GlobalConfigUtil.kafkaZookeeperConnect;
        String instance = GlobalConfigUtil.canalInstance;
        String username = GlobalConfigUtil.mysqlUsername;
        String password = GlobalConfigUtil.mysqlPassword;
        //获取Canal连接
        CanalConnector conn = CanalConnectors.newClusterConnector(zookeeper,instance,username,password);

        // 从binlog中读取数据
        try {
            // 连接cannal
            conn.connect();
            //订阅实例中所有的数据库和表

            /**
             * 1. 所有表：.* or .*\\..*
             * 2. canal schema下所有表： canal\\..*
             * 3. canal下的以canal打头的表：canal\\.canal.*
             * 4. canal schema下的一张表：canal.test1
             * 5. 多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
             */
//            conn.subscribe("canal_manager.snap_payable");
            conn.subscribe(".*\\..*");
            // 回滚到未进行ack的地方
            conn.rollback();

            while(true){
                // 获取数据
                Message message = conn.getWithoutAck(1000);    // 获取指定数量的数据

                long id = message.getId();
                int size = message.getEntries().size();
                if (id == -1 || size == 0) {
                    //没有读取到任何数据
                } else {
                    //有数据，那么解析binlog日志
                    analysis(message.getEntries());
                }
                // 确认消息
                conn.ack(message.getId());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.disconnect();
        }
    }

    public void analysis(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            // 只解析mysql事务的操作，其他的不解析
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            // 那么解析binlog
            CanalEntry.RowChange rowChange = null;

            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
//            SendThread sendThread = new SendThread("canaltest","1",rowChange.getSql());
//            sendThread.start();
            // 获取操作类型字段（增加  删除  修改）
            CanalEntry.EventType eventType = rowChange.getEventType();
            // 获取binlog文件名称
            String logfileName = entry.getHeader().getLogfileName();
            // 读取当前操作在binlog文件的位置
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取当前操作所属的数据库
            String dbName = entry.getHeader().getSchemaName();
            // 获取当前操作所属的表
            String tableName = entry.getHeader().getTableName();//当前操作的是哪一张表
            System.out.println(tableName);
            long timestamp = entry.getHeader().getExecuteTime();//执行时间

            //非行操作
            if (eventType== CanalEntry.EventType.ERASE||eventType==CanalEntry.EventType.TRUNCATE||eventType==CanalEntry.EventType.ALTER||eventType== CanalEntry.EventType.RENAME
                    ||eventType== CanalEntry.EventType.DINDEX||eventType== CanalEntry.EventType.CINDEX){
            }

            // 解析操作的行数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 删除操作
                if (eventType == CanalEntry.EventType.DELETE) {
                    // 获取删除之前的所有列数据
                    dataDetails(rowData.getBeforeColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType,timestamp);
                }
                // 新增操作  或者修改
                else{//(eventType == CanalEntry.EventType.INSERT)
                    // 获取新增之后的所有列数据
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType,timestamp);
                }

            }
        }
    }

     String dataDetails(List<CanalEntry.Column> columns,
                            String logFileName,
                            Long logFileOffset,
                            String dbName,
                            String tableName,
                            CanalEntry.EventType eventType,
                            long timestamp) {
        // 找到当前那些列发生了改变  以及改变的值
        StringBuilder stringBuilder = new StringBuilder();
        if(null!=columns){
            if(eventType==CanalEntry.EventType.DELETE) {
                for (CanalEntry.Column column : columns) {
                    stringBuilder.append("\"" + column.getValue() + "\"");
                    break;
                }
            }else {
                for (CanalEntry.Column column : columns) {
                    stringBuilder.append("\""+column.getValue()+"\"\t");
                }
            }
        }

        SimpleDateFormat simpleDateFormat  = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        String date = simpleDateFormat.format(timestamp);

        String key = UUID.randomUUID().toString();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("logFileName", logFileName);
        jsonObject.put("logFileOffset", logFileOffset);
        jsonObject.put("dbName", dbName);
        jsonObject.put("tableName", tableName);
        jsonObject.put("eventType", eventType);
        jsonObject.put("columnValueList",stringBuilder.toString()+"\""+date+"\"\t\""+logFileOffset+"\"\n");
//        jsonObject.put("emptyCount", emptyCount);
        jsonObject.put("timestamp", timestamp);

        // 拼接所有binlog解析的字段
        String data = JSON.toJSONString(jsonObject);
        System.out.println(jsonObject.getString("columnValueList"));
//      System.out.println(data);
        kafkaManager.sendMessage(data);

//      SendThread sendThread = new SendThread("canaltest",key,data);
//      sendThread.start();
        // 解析后的数据发送到kafka
        return data;
    }

    static class ColumnValuePair {
        private String columnName;
        private String columnValue;
        private Boolean isValid;

        public ColumnValuePair(String columnName, String columnValue, Boolean isValid) {
            this.columnName = columnName;
            this.columnValue = columnValue;
            this.isValid = isValid;
        }

        public String getColumnName() { return columnName; }
        public void setColumnName(String columnName) { this.columnName = columnName; }
        public String getColumnValue() { return columnValue; }
        public void setColumnValue(String columnValue) { this.columnValue = columnValue; }
        public Boolean getIsValid() { return isValid; }
        public void setIsValid(Boolean isValid) { this.isValid = isValid; }

    }

    public static BlockingQueue<KafkaProducer<String,String>> queue = new LinkedBlockingDeque<>(50);

    static {

        for (int i = 0 ; i < 5;i++){
            KafkaProducer<String,String> kafkaProducer =createProducer();
            queue.add(kafkaProducer);
        }
    }

    private static KafkaProducer<String , String> createProducer(){
        Properties properties = new Properties();

        properties.put("bootstrap.servers" , GlobalConfigUtil.kafkaBootstrapServers);
        properties.put("zookeeper.connect" , GlobalConfigUtil.kafkaZookeeperConnect);
//        properties.put("serializer.class" , StringEncoder.class.getName());
        properties.put("retries","0");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("linger.ms","0");
        properties.put("buffer.memory","33554432");
        properties.put("max.request.size","16777216");

        return new KafkaProducer<>(properties);
    }


    static class SendThread extends Thread{
        String topic;
        String key;
        String data;
        public SendThread(String topic,String key,String data){
            this.topic = topic;
            this.key=key;
            this.data=data;
        }

        public void run(){
            try{
                KafkaProducer<String,String> kafkaProducer = queue.take();
                kafkaProducer.send(new ProducerRecord<>(topic,key,data));
                queue.put(kafkaProducer);

            }catch (Exception e){
                e.printStackTrace();
            }

        }

    }
}
