-- Create the calendar_AF table
CREATE TABLE IF NOT EXISTS QP_DW_RMALLOC.calendar_AF (
    Sector VARCHAR(10) NOT NULL,
    DepartureDate DATE NOT NULL,
    AverageFare DECIMAL(10,2),
    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Sector, DepartureDate)
);

-- Create performance indexes
CREATE INDEX idx_calendar_af_date ON QP_DW_RMALLOC.calendar_AF(DepartureDate);
CREATE INDEX idx_calendar_af_sector ON QP_DW_RMALLOC.calendar_AF(Sector);
CREATE INDEX idx_calendar_af_date_sector ON QP_DW_RMALLOC.calendar_AF(DepartureDate, Sector);