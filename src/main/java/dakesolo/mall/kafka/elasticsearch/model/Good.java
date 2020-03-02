package dakesolo.mall.kafka.elasticsearch.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

@Document( indexName = "binlog_product-a_good", type = "_doc")
public class Good {
    @Id
    @Field(type = FieldType.Integer)
    private Long goodID;
    @Field(type = FieldType.Text)
    private String title;
    @Field(type = FieldType.Integer)
    private Integer stock;
    @Field(type = FieldType.Integer)
    private Integer status;
    @Field(type = FieldType.Integer)
    private Integer catID;


    //@Field(type = FieldType.Date, format = DateFormat.custom, pattern ="yyyy-MM-dd'T'HH:mm:ssZ")
    //@JsonFormat(pattern="yyyy-MM-dd HH:mm:ss",timezone="GMT+8")
    //@JsonFormat (shape = JsonFormat.Shape.STRING, pattern ="yyyy-MM-dd")
    //@DateTimeFormat(pattern="yyyy-MM-dd HH:mm:ss")
    @Field(type = FieldType.Date, format = DateFormat.custom, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
    private Date createTime;

    @Field(type = FieldType.Date, format = DateFormat.custom, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
    private Date updateTime;

    @Field(type = FieldType.Date, format = DateFormat.custom, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
    private Long setTime;

    @Field(type = FieldType.Date, format = DateFormat.custom, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
    private Long modifyTime;

    @Transient
    private String statusText;

    public String getStatusText() {
        if(getStatus() == 1) {
            return "已上线";
        }
        return "已下线";
    }

    public Long getGoodID() {
        return goodID;
    }

    public void setGoodID(Long goodID) {
        this.goodID = goodID;
    }

    public Integer getCatID() {
        return catID;
    }

    public void setCatID(Integer catID) {
        this.catID = catID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getStock() {
        return stock;
    }

    public void setStock(Integer stock) {
        this.stock = stock;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Long getSetTime() {
        return setTime;
    }

    public void setSetTime(Long setTime) {
        this.setTime = setTime;
    }

    public Long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Long modifyTime) {
        this.modifyTime = modifyTime;
    }

    public void setStatusText(String statusText) {
        this.statusText = statusText;
    }

    @Override
    public String toString() {
        return "Good{" +
                "goodID=" + goodID +
                ", title='" + title + '\'' +
                ", stock=" + stock +
                ", status=" + status +
                ", catID=" + catID +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", statusText='" + statusText + '\'' +
                '}';
    }
}