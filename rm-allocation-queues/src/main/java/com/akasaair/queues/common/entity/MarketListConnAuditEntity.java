package com.akasaair.queues.common.entity;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString(callSuper = true)
public class MarketListConnAuditEntity extends MarketListConnectionsEntity {
    private String sector;
    private String flightNo;
    private String date;
    private String userName;
    private String changedAt;
    private boolean newRecord;
}
