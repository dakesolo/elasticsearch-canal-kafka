package dakesolo.mall.kafka.elasticsearch.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

@Document( indexName = "mall-product_cat", type = "cat")
public class Cat {
    @Id
    private Long catID;
    private String title;

    public Long getCatID() {
        return catID;
    }

    public void setCatID(Long catID) {
        this.catID = catID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}