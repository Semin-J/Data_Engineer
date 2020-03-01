`dim_immigration_table`
Contains filtered data from the I94 immigration data.
- i94yr = 4-digit year
- i94mon = numeric month
- i94cit = 3-digit code of origin city
- i94port = 3-character code of destination city
- arrdate = arrival date
- i94mode = 1-digit travel code
- depdate = departure date
- i94visa = reason for immigration

`dim_temp_city_table`
Contains city temperature data.
- AverageTemperature = average temperature
- City = city name
- Country = country name
- Latitude = latitude
- Longitude = longitude
- i94port = 3-character code of destination city

`fact_immigration_temp_table`
Contains information from the I94 immigration data joined with the temperature by city data on i94port (city code).
- i94yr = 4-digit year
- i94mon = numeric month
- i94cit = 3-digit code of origin city
- i94port = 3-character code of destination city
- arrdate = arrival date
- i94mode = 1-digit travel code
- depdate = departure date
- i94visa = reason for immigration
- AverageTemperature = average temperature of destination city