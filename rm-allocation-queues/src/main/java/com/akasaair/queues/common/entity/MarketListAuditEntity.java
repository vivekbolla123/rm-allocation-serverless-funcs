package com.akasaair.queues.common.entity;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString(callSuper = true)
public class MarketListAuditEntity extends MarketListEntity {
    private String sector;
    private String date;
    private String userName;
    private String changedAt;
}
