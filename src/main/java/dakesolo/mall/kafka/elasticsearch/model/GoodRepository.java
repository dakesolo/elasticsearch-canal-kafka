package dakesolo.mall.kafka.elasticsearch.model;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public interface GoodRepository extends ElasticsearchRepository<Good, Long> { }
