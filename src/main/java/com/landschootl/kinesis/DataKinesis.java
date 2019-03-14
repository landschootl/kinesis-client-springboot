package com.landschootl.kinesis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataKinesis {

    @JsonProperty("key")
    public String key;

    @JsonProperty("value1")
    public Integer value1;

    @JsonProperty("value2")
    public Integer value2;

    @JsonProperty("value3")
    public Integer value3;

    @JsonProperty("value4")
    public Integer value4;
}