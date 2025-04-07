CREATE TABLE IF NOT EXISTS QP_DW_RMALLOC.expected_no_shows (
    DepartureDate VARCHAR(10) NOT NULL,
    Sector VARCHAR(6) NOT NULL,
    FlightNumber VARCHAR(10) NOT NULL,
    TimeCluster VARCHAR(20) NOT NULL,
    BookedPax INT NOT NULL,
    ExpectedNoShows INT NOT NULL,
    GeneratedAt DATETIME NOT NULL,
    PRIMARY KEY (DepartureDate, FlightNumber, Sector)
)