--The data we're looking for is currently updated on a day-to-day basis
--The goal is to first aggregate the required data on a week-to-week basis, then join them

--First we're aggregating the data from the COVID death table
--The where clause is to prevent continents or 'world' or similar extraneous data from being included
--The current goal is to get weekly data, the new deaths are standardized and the reproduction rate is a good metric of covid containability
--The purpose of the having clause is to ensure at least 4 non-null values per week
--We arbitrarily chose max population since it wouldn't fluxuate much over a week instead of adding it to group by
SELECT
location,
MAX(population) AS population,
EXTRACT(YEAR FROM date) * 100 + EXTRACT(WEEK FROM date) AS year_week,
SUM(new_deaths_per_million) AS weekly_new_deaths_per_million,
AVG(reproduction_rate) AS weekly_average_rr
FROM "CovidDeaths"
WHERE continent IS NOT NULL
GROUP BY location, year_week
HAVING 
COUNT(CASE WHEN new_deaths_per_million IS NOT NULL THEN 1 END) >= 4
AND COUNT(CASE WHEN reproduction_rate IS NOT NULL THEN 1 END) >= 4
ORDER BY year_week;

--Now we want to properly partition the covid vaccination dataset
--The rationale for most of the decisions here are the same as the COVID set
--I also took some control variables to measure the importance of 
SELECT
location,
EXTRACT(YEAR FROM date) * 100 + EXTRACT(WEEK FROM date) AS year_week,
AVG(stringency_index) AS weekly_avg_stringency_index,
AVG(positive_rate) AS weekly_avg_positive_rate,
MAX(population_density) AS population_density,
MAX(median_age) AS median_age,
MAX(gdp_per_capita) AS gdp_per_capita
FROM "CovidVaccinations"
WHERE continent IS NOT NULL
GROUP BY location, year_week
HAVING
COUNT(CASE WHEN stringency_index IS NOT NULL THEN 1 END) >= 4 AND
COUNT(CASE WHEN positive_rate IS NOT NULL THEN 1 END) >= 4 
ORDER BY year_week;


--Now we're essentially just going to be combining the previous two queries via CTE's to get the proper dataset
--The only addition we're adding is the introduction of a lagged_stringency_index because policies may take a couple of weeks to show their true effects.
WITH weekly_covid_death AS
(
SELECT
location,
MAX(population) AS population,
EXTRACT(YEAR FROM date) * 100 + EXTRACT(WEEK FROM date) AS year_week,
SUM(new_deaths_per_million) AS weekly_new_deaths_per_million,
AVG(reproduction_rate) AS weekly_average_rr
FROM "CovidDeaths"
WHERE continent IS NOT NULL
GROUP BY location, year_week
HAVING 
COUNT(CASE WHEN new_deaths_per_million IS NOT NULL THEN 1 END) >= 4
AND COUNT(CASE WHEN reproduction_rate IS NOT NULL THEN 1 END) >= 4
),
weekly_covid_vaccination AS
(
SELECT
location,
EXTRACT(YEAR FROM date) * 100 + EXTRACT(WEEK FROM date) AS year_week,
AVG(stringency_index) AS weekly_avg_stringency_index,
AVG(positive_rate) AS weekly_avg_positive_rate,
MAX(population_density) AS population_density,
MAX(median_age) AS median_age,
MAX(gdp_per_capita) AS gdp_per_capita
FROM "CovidVaccinations"
WHERE continent IS NOT NULL
GROUP BY location, year_week
HAVING
COUNT(CASE WHEN stringency_index IS NOT NULL THEN 1 END) >= 4 AND
COUNT(CASE WHEN positive_rate IS NOT NULL THEN 1 END) >= 4 
)
SELECT
CD.location,
CD.population,
CD.year_week,
CD.weekly_new_deaths_per_million,
CD.weekly_average_rr,
CV.weekly_avg_stringency_index,
LAG(CV.weekly_avg_stringency_index, 2) OVER (PARTITION BY CV.location ORDER BY CV.year_week) AS lagged_stringency_2weeks,
CV.weekly_avg_positive_rate,
CV.population_density,
CV.median_age,
CV.gdp_per_capita
FROM weekly_covid_death CD
INNER JOIN weekly_covid_vaccination CV
ON CD.location = CV.location AND CD.year_week = CV.year_week
ORDER BY CD.year_week;

--4 SQL scripts for Tableau visualization
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths), SUM(new_deaths)/SUM(new_cases) * 100 as DeathPercentage
FROM "CovidDeaths"
WHERE continent IS NOT NULL
ORDER BY 1,2;

SELECT location, SUM(new_deaths) AS total_deaths
FROM "CovidDeaths"
WHERE continent IS NULL
AND location NOT IN ('World', 'European Union', 'Internation')
GROUP BY location
ORDER BY total_deaths DESC;

SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS percent_pop_infected
FROM "CovidDeaths"
GROUP BY location, population
ORDER BY percent_pop_infected DESC NULLS LAST;

SELECT location, population, date, MAX(total_cases) AS highest_infec_count, MAX((total_cases/population))*100 AS percent_pop_infected
FROM "CovidDeaths"
GROUP BY location, population, date
ORDER BY percent_pop_infected DESC NULLS LAST;




