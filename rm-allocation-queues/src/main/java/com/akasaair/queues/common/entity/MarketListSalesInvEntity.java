package com.akasaair.queues.common.entity;

// If you use Lombok, you can add @Data and @Builder annotations.
public class MarketListSalesInvEntity {
    private String origin;
    private String destin;
    private String flightNumber;
    private String perStart;
    private String perEnd;
    private String dow;
    private String curveId;
    private String saleFlag;

    // Static builder for easy object creation
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String origin;
        private String destin;
        private String flightNumber;
        private String perStart;
        private String perEnd;
        private String dow;
        private String curveId;
        private String saleFlag;

        public Builder origin(String origin) { this.origin = origin; return this; }
        public Builder destin(String destin) { this.destin = destin; return this; }
        public Builder flightNumber(String flightNumber) { this.flightNumber = flightNumber; return this; }
        public Builder perStart(String perStart) { this.perStart = perStart; return this; }
        public Builder perEnd(String perEnd) { this.perEnd = perEnd; return this; }
        public Builder dow(String dow) { this.dow = dow; return this; }
        public Builder curveId(String curveId) { this.curveId = curveId; return this; }
        public Builder saleFlag(String saleFlag) { this.saleFlag = saleFlag; return this; }

        public MarketListSalesInvEntity build() {
            MarketListSalesInvEntity entity = new MarketListSalesInvEntity();
            entity.origin = this.origin;
            entity.destin = this.destin;
            entity.flightNumber = this.flightNumber;
            entity.perStart = this.perStart;
            entity.perEnd = this.perEnd;
            entity.dow = this.dow;
            entity.curveId = this.curveId;
            entity.saleFlag = this.saleFlag;
            return entity;
        }
    }

    // Getters for all fields
    public String getOrigin() { return origin; }
    public String getDestin() { return destin; }
    public String getFlightNumber() { return flightNumber; }
    public String getPerStart() { return perStart; }
    public String getPerEnd() { return perEnd; }
    public String getDow() { return dow; }
    public String getCurveId() { return curveId; }
    public String getSaleFlag() { return saleFlag; }
}