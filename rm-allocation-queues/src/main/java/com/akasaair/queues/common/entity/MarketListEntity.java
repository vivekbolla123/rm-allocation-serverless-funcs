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
@Table(name = "market_list")
@EntityListeners(AuditingEntityListener.class)
public class MarketListEntity {
    @Column(name = "UUID")
    public String uuid;
    @Column(name = "Origin")
    public String origin;
    @Column(name = "Destin")
    public String destination;
    @Column(name = "FlightNumber")
    public String flightNo;
    @Column(name = "PerType")
    public String perType;
    @Column(name = "PerStart")
    public String startDate;
    @Column(name = "PerEnd")
    public String endDate;
    @Column(name = "DOW")
    public String dayOfWeek;
    @Column(name = "TimeWindowStart")
    public String startTime;
    @Column(name = "TimeWindowEnd")
    public String endTime;
    @Column(name = "CurveID")
    public String curveId;
    @Column(name = "CarrExlusionB2C")
    public String carrExclusionB2C;
    @Column(name = "CarrExlusionB2B")
    public String carrExclusionB2B;
    @Column(name = "flightExclusionB2C")
    public String flightExclusionB2C;
    @Column(name = "flightExclusionB2B")
    public String flightExclusionB2B;
    @Column(name = "fareAnchor")
    public String fareAnchor;
    @Column(name = "hardAnchor")
    public String hardAnchor;
    @Column(name = "plfThreshold")
    public String plfThreshold;
    @Column(name = "fareOffset")
    public String fareOffset;
    @Column(name = "FirstRBDAlloc")
    public String firstAllocation;
    @Column(name = "OtherRBDAlloc")
    public String otherAllocation;
    @Column(name = "B2BBackstop")
    public String b2bBackstop;
    @Column(name = "B2CBackstop")
    public String b2cBackstop;
    @Column(name = "B2BFactor")
    public String b2bFactor;
    @Column(name = "SkippingFactor")
    public String skippingFactor;
    @Column(name = "analystName")
    public String analystName;
    @Column(name = "DaySpan")
    public String daySpan;
    @Column(name = "openingFares")
    public String openingFares;
    @Column(name = "AutoTimeRangeFlag")
    public String autoTimeRangeFlag;
    @Column(name = "OverBooking")
    public String overBooking;
    @Column(name = "obSeats")
    public String OBSeats;
    @Column(name = "obFare")
    public String OBFare;
    @Column(name = "rbdPushFlag")
    public String rbdPushFlag;
    @Column(name = "profileFares")
    public String profileFares;
    @Column(name = "B2BTolerance")
    public String b2bTolerance;
    @Column(name = "B2CTolerance")
    public String b2cTolerance;
    @Column(name = "distressInventoryFlag")
    public String distressInventoryFlag;
    @Column(name = "seriesBlock")
    public String seriesBlock;
    @Column(name = "autoGroup")
    public String autoGroup;
    @Column(name = "autoBackstopFlag")
    public String autoBackstopFlag;
    @Column(name = "tbfFlag")
    public String tbfFlag;

}
