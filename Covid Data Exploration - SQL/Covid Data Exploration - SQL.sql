-- IMPORT DATA TO DATABASE

-- create database
CREATE DATABASE covid_proj;
USE covid_proj;

-- create covid_deaths table
CREATE TABLE covid_deaths
(
	iso_code	TEXT,
    continent	TEXT,
    location	TEXT,
    date		DATE,
    population	INT,
    total_cases	INT,
    new_cases	INT,
    new_cases_smoothed	FLOAT,
    total_deaths		INT,
    new_deaths			INT,
    new_deaths_smoothed	FLOAT,
    total_cases_per_million	FLOAT,
    new_cases_per_million	FLOAT,
    new_cases_smoothed_per_million	FLOAT,
    total_deaths_per_million	FLOAT,
    new_deaths_per_million		FLOAT,
    new_deaths_smoothed_per_million	FLOAT,
	reproduction_rate	FLOAT,
    icu_patients	FLOAT,
    icu_patients_per_million	FLOAT,
    hosp_patients	FLOAT,
    hosp_patients_per_million	FLOAT,
    weekly_icu_admissions	FLOAT,
    weekly_icu_admissions_per_million	FLOAT,
    weekly_hosp_admissions	FLOAT,
    weekly_hosp_admissions_per_million	FLOAT
    ) ENGINE = InnoDB;
DESCRIBE covid_deaths;

-- create covid_vacc table
CREATE TABLE covid_vacc
(
	iso_code	TEXT,
    continent	TEXT,
    location	TEXT,
    date		DATE,
    new_test	INT,
    total_tests	INT,
    total_tests_per_thousand	FLOAT,
    new_tests_per_thousand	FLOAT,
    new_tests_smoothed	FLOAT,
    new_tests_smoothed_per_thousand	FLOAT,
    positive_rate	FLOAT,
    tests_per_case	FLOAT,
    tests_units	TEXT,
    total_vaccinations	FLOAT,
    people_vaccinated	INT,
    people_fully_vaccinated	INT,
    total_boosters	INT,
    new_vaccinations	INT,
    new_vaccinations_smoothed	FLOAT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred	FLOAT,
    people_fully_vaccinated_per_hundred	FLOAT,
    total_boosters_per_hundred	FLOAT,
    new_vaccinations_smoothed_per_million	FLOAT,
    new_people_vaccinated_smoothed	FLOAT,
    new_people_vaccinated_smoothed_per_hundred	FLOAT,
    stringency_index	FLOAT,
    population_density	FLOAT,
    median_age	FLOAT,
    aged_65_older	FLOAT,
    aged_70_older	FLOAT,
    gdp_per_capita	FLOAT,
    extreme_poverty	FLOAT,
    cardiovasc_death_rate	FLOAT,
    diabetes_prevalence	FLOAT,
    female_smokers	FLOAT,
    male_smokers	FLOAT,
    handwashing_facilities	FLOAT,
    hospital_beds_per_thousand	FLOAT,
    life_expectancy	FLOAT,
    human_development_index FLOAT,
    excess_mortality_cumulative_absolute	FLOAT,
    excess_mortality_cumulative	FLOAT,
    excess_mortality	FLOAT,
    excess_mortality_cumulative_per_million	FLOAT
) ENGINE = InnoDB;
DESCRIBE covid_vacc;

-- to enable MySQL to insert empty data as 0
SET sql_mode="" ;

-- import CSV file
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Data/CovidDeaths.csv'
INTO TABLE covid_deaths
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Data/CovidVaccinations.csv'
INTO TABLE covid_vacc
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- PROJECT START

SELECT * FROM covid_deaths LIMIT 1000;
SELECT * FROM covid_vacc LIMIT 1000;

-- checking null data in continent column
SELECT *
FROM covid_deaths
WHERE continent LIKE ''
ORDER BY 3, 4;

-- null data in continent column is ignored in this project

-- data overview
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1, 2;

-- TOTAL CASES VS TOTAL DEATHS
-- death percentage by country and time
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1, 2;

-- TOTAL CASES VS POPULATION
-- percentage of population infected with covid
SELECT location, date, population, total_cases, (total_cases/population)*100 AS percent_population_infected
FROM covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1, 2;

-- countries with highest infection rate compared to population
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS percent_population_infected
FROM covid_deaths
WHERE continent NOT LIKE ''
GROUP BY location, population
ORDER BY percent_population_infected DESC;

-- countries with highest death count per population
SELECT location, MAX(total_deaths) AS total_death_count
FROM covid_deaths
WHERE continent NOT LIKE ''
GROUP BY location
ORDER BY total_death_count DESC;

-- BREAKDOWN BY CONTINENT
-- contintents with the highest death count per population
SELECT continent, MAX(total_deaths) AS total_death_count
FROM covid_deaths
WHERE continent NOT LIKE ''
GROUP BY 1
ORDER BY 2 DESC;

-- GLOBAL NUMBERS
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths
FROM covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1, 2;

-- TOTAL POPULATIONS VS VACCINATIONS
-- using join for two tables, using window function
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaccinated
FROM covid_deaths AS dea
JOIN covid_vacc AS vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent NOT LIKE ''
ORDER BY 2, 3;

-- using CTE
WITH PopvsVac (continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
AS
(
	SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
		SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_people_vaccinated
	FROM covid_deaths AS dea
	JOIN covid_vacc AS vac
		ON dea.location = vac.location
		AND dea.date = vac.date
	WHERE dea.continent NOT LIKE ''
	ORDER BY 2, 3
)
SELECT *, (rolling_people_vaccinated/population)*100 AS rolling_percent_vaccinated
FROM PopvsVac;

-- creating view
CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM covid_deaths AS dea
JOIN covid_vacc AS vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent NOT LIKE ''
ORDER BY 2, 3;
