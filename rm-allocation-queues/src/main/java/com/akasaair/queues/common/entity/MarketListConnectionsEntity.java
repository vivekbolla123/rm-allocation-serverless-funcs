package com.akasaair.queues.common.entity;

import lombok.*;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.Table;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@Entity()
@Table(name = "market_list_connections")
@EntityListeners(AuditingEntityListener.class)
public class MarketListConnectionsEntity {
    @Column(name = "UUID")
    private String uuid;
    @Column(name = "Sector1")
    private String sector1;
    @Column(name = "Sector2")
    private String sector2;
    @Column(name = "Flight1")
    private String flight1;
    @Column(name = "Flight2")
    private String flight2;
    @Column(name = "PerType")
    private String perType;
    @Column(name = "PerStart")
    private String startDate;
    @Column(name = "PerEnd")
    private String endDate;
    @Column(name = "DOW")
    private String dayOfWeek;
    @Column(name = "Price_Strategy")
    private String priceStrategy;
    @Column(name = "Discount_Value")
    private String discountValue;
    @Column(name = "FirstRBDAlloc")
    private String firstAllocation;
    @Column(name = "OtherRBDAlloc")
    private String otherAllocation;
    @Column(name = "B2BBackstop")
    private String b2bBackstop;
    @Column(name = "B2CBackstop")
    private String b2cBackstop;
    @Column(name = "B2BFactor")
    private String b2bFactor;
    @Column(name = "SkippingFactor")
    private String skippingFactor;
    @Column(name = "Outbound_stop")
    private String outboundStop;
    @Column(name = "Outbound_duration")
    private String outboundDuration;
    @Column(name = "Currency")
    private String currency;
    @Column(name = "fareAnchor")
    private String fareAnchor;
    @Column(name = "Offset")
    private String offset;
    @Column(name = "DiscountFlag")
    private String discountFlag;
    @Column(name = "analystName")
    private String analystName;

}

