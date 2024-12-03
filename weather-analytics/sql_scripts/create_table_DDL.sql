CREATE TABLE [DWDEV].[Weather].[CountryDetails]
(
    [CountryCode] [varchar](8000)  NULL,
    [Country] [varchar](8000)  NULL,
    [Region] [varchar](8000)  NULL
)
GO



CREATE TABLE [LHDEV].[atmospheredet]
(
AtmosphereID string,
observation_time timestamp,
temperature string,
Humidity string,
CloundCover string,
wind_direction string,
wind_degree string,
wind_speed string,
is_day string,
is_latest int,
WeatherID int
);

CREATE TABLE [LHDEV].[countrydetfinal]
(
CountryCode string,
City string,
Country string,
Region string,
Latitude decimal(8,6),
Longitude decimal(9,6)
);

CREATE TABLE [LHDEV].[countrymstr]
(
CountryCode string,
WeatherID int,
AtmosphereID string,
Load_TS timestamp
);

CREATE TABLE [LHDEV].[weatherdet]
(
WeatherID int,
weather_descriptions string,
);