package net.khaibq.addon.model;

import lombok.Data;

@Data
public class ApiResponse {
    private String status;
    private Integer code;
    private Query query;
    private Items items;
    private Errors errors;

    @Data
    public static class Query {
        private String processId;
        private String dbSchemaId;
        private String importId;
    }

    @Data
    public static class Items {
        private Integer nowCondition;
        private Integer progress;
        private Integer succeedCount;
        private Integer failureCount;
    }

    @Data
    public static class Errors {
        private Integer code;
        private String msg;
    }
}
