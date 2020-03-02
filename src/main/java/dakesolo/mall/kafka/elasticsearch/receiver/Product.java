package dakesolo.mall.kafka.elasticsearch.receiver;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dakesolo.mall.kafka.elasticsearch.model.GoodRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
@PropertySource("classpath:deliver.properties")
public class Product {

    private static final Logger logger = LoggerFactory.getLogger(Product.class);

    @Autowired
    GoodRepository goodRepository;

    @Autowired
    ObjectMapper objectMapper;


    @Qualifier("elasticsearchTemplate")
    @Autowired
    ElasticsearchOperations elasticsearchOperations;

    @KafkaListener(topics = "${deliver.kafka.topics}")
    public void Binlog_Product(ConsumerRecord<?, ?> record) throws IOException {
        logger.info("receive kafka message....");
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (!kafkaMessage.isPresent()) {
            logger.warn("message:" + "no message");
            return;
        }

        Object message = kafkaMessage.get();
        JsonNode jsonNode = objectMapper.readValue(message.toString(), JsonNode.class);
        String type = jsonNode.get("type").asText();
        JsonNode arrNode = jsonNode.get("data");
        //索引名，elasticsearch index 当前索引为小写
        String indexName = (record.topic() + "-" +jsonNode.get("table").asText()).toLowerCase();
        //主键，默认是pkNames的第一个
        String pkIDName = jsonNode.get("pkNames").get(0).asText();

        if (type.equals("INSERT")) {
            List<IndexQuery> queryList = new ArrayList<>();
            for (JsonNode objNode : arrNode) {
                ObjectNode objectNode = (ObjectNode) objNode;
                objectNode.put("modifyTime", System.currentTimeMillis());
                objectNode.put("setTime", System.currentTimeMillis());
                IndexQuery indexQuery = new IndexQueryBuilder()
                        .withId(objNode.get(pkIDName).asText())
                        .withIndexName(indexName)
                        .withType("_doc")
                        .withSource(objectNode.toString())
                        .build();
                queryList.add(indexQuery);
            }
            elasticsearchOperations.bulkIndex(queryList);
            logger.info(type + " success " + queryList.size());
        }
        else if (type.equals("UPDATE")) {
            List<UpdateQuery> queryList = new ArrayList<>();
            for (JsonNode objNode : arrNode) {
                ObjectNode objectNode = (ObjectNode) objNode;
                //更新额外字段
                objectNode.put("modifyTime", System.currentTimeMillis());
                UpdateRequest updateRequest = new UpdateRequest();
                updateRequest.doc(objectNode.toString(), XContentType.JSON);
                UpdateQuery updateQuery = new UpdateQueryBuilder()
                        .withId(objNode.get(pkIDName).asText())
                        .withIndexName(indexName)
                        .withType("_doc")
                        .withUpdateRequest(updateRequest)
                        //如果没有记录则需要添加
                        .withDoUpsert(true)
                        .build();
                queryList.add(updateQuery);
            }
            elasticsearchOperations.bulkUpdate(queryList);
            logger.info(type + " success " + queryList.size());
        }
        else if (type.equals("DELETE")) {
            for (JsonNode objNode : arrNode) {
                //直接根据id删除文档速度快
                //参考：https://docs.spring.io/spring-data/elasticsearch/docs/current/api/org/springframework/data/elasticsearch/core/ElasticsearchOperations.html#delete-java.lang.String-java.lang.String-java.lang.String-
                elasticsearchOperations.delete(indexName, "_doc", objNode.get(pkIDName).asText());
                logger.info(type + " success 1");
            }
        }
        else {
            logger.warn("no INSERT, no UPDATE, no DELETE, so other?message=" + message.toString());
        }
    }



    /*@KafkaListener(topics = {"Binlog_Product"})
    public void Binlog_Product(ConsumerRecord<?, ?> record) throws IOException {
        logger.info("receive kafka message....");
        System.out.println("receive kafka message....");
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (!kafkaMessage.isPresent()) {
            logger.warn("message:" + "no message");
            return;
        }

        Object message = kafkaMessage.get();
        logger.info("message:" + message.toString());
        JsonNode jsonNode = objectMapper.readValue(message.toString(), JsonNode.class);
        String type = jsonNode.get("type").asText();
        JsonNode arrNode = jsonNode.get("data");
        if (type.equals("INSERT") || type.equals("UPDATE")) {
            if (arrNode.isArray()) {
                for (JsonNode objNode : arrNode) {
                    Good good = objectMapper.treeToValue(objNode, Good.class);
                    Long time = Long.valueOf(jsonNode.get("ts").asText());
                    good.setModifyTime(time);
                    if(type.equals("INSERT")) {
                        good.setSetTime(time);
                    }
                    goodRepository.save(good);
                    logger.info(type + ":" + good.getGoodID() + " success");
                }
            }
        }
        else if(type.equals("DELETE")) {
            if (arrNode.isArray()) {
                for (JsonNode objNode : arrNode) {
                    Good good = objectMapper.treeToValue(objNode, Good.class);
                    goodRepository.delete(good);
                    logger.info(type + ":" + good.getGoodID() + " success");
                }
            }
        }
    }*/
}
