/*
Für Data Exploration nehme ich die Datensätze über Covid19 aus folgendem URL: https://ourworldindata.org/covid-deaths
Folgendes muss in den CSV Dateien abgeändert werden, bevor sie importiert werden können, sonst entstehen errors:
Dezimal-Separator auf "." und Tausender-Separator auf "," einstellen
Ebenso muss Datum auf "JJJJ-MM-TT" umgeändert werden

Tabellen mit header dienen als Grundgerüst, das später mit Daten gefüllt wird
Auf genaue Schreibweise, Reihenfolge der Spalten und ihre Datentypen achten
Erste Tabelle heißt coviddeaths
*/
DROP TABLE IF EXISTS coviddeaths;
CREATE TABLE coviddeaths 
(
	iso_code VARCHAR(10),
	continent VARCHAR(50),
	location VARCHAR(100),
	date DATE,
	total_cases INT,
	new_cases INT,
	new_cases_smoothed DOUBLE,
	total_deaths INT,
	new_deaths INT,
	new_deaths_smoothed DOUBLE,
	total_cases_per_million DOUBLE,
	new_cases_per_million DOUBLE,
	new_cases_smoothed_per_million DOUBLE,
	total_deaths_per_million DOUBLE,
	new_deaths_per_million DOUBLE,
	new_deaths_smoothed_per_million DOUBLE,
	reproduction_rate DOUBLE,
	icu_patients INT,
	icu_patients_per_million DOUBLE,
	hosp_patients INT,
	hosp_patients_per_million DOUBLE,
	weekly_icu_admissions DOUBLE,
	weekly_icu_admissions_per_million DOUBLE,
	weekly_hosp_admissions DOUBLE,
	weekly_hosp_admissions_per_million DOUBLE,
	new_tests INT,
	total_tests INT,
	total_tests_per_thousand DOUBLE,
	new_tests_per_thousand DOUBLE,
	new_tests_smoothed INT,
	new_tests_smoothed_per_thousand DOUBLE,
	positive_rate DOUBLE,
	tests_per_case DOUBLE,
	tests_units VARCHAR(50),
	total_vaccinations INT,
	people_vaccinated INT,
	people_fully_vaccinated INT,
	new_vaccinations INT,
	new_vaccinations_smoothed INT,
	total_vaccinations_per_hundred DOUBLE,
	people_vaccinated_per_hundred DOUBLE,
	people_fully_vaccinated_per_hundred DOUBLE,
	new_vaccinations_smoothed_per_million INT,
	stringency_index DOUBLE,
	population BIGINT, -- bei int kommt error, aber bigint kann sehr große Zahlen handeln
	population_density DOUBLE,
	median_age DOUBLE,
	aged_65_older DOUBLE,
	aged_70_older DOUBLE,
	gdp_per_capita DOUBLE,
	extreme_poverty DOUBLE,
	cardiovasc_death_rate DOUBLE,
	diabetes_prevalence DOUBLE,
	female_smokers DOUBLE,
	male_smokers DOUBLE,
	handwashing_facilities DOUBLE,
	hospital_beds_per_thousand DOUBLE,
	life_expectancy DOUBLE,
	human_development_index DOUBLE
);

/*
Daten werden von CSV in Tabellen importiert und load data infile ist deutlich schneller als mit import wizard
Gleichzeitig wird NULL-Handling durchgeführt, sodass auf Datenintegrität und Genauigkeit in der Datenbank gewährleistet ist.
Auch wird mit Daten-Konvertierung darauf geachtet, dass die Dateiformaten funktionieren.
*/
LOAD DATA INFILE 'path\\CovidDeaths.csv'
INTO TABLE coviddeaths
FIELDS TERMINATED BY ';' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
	@iso_code,
	@continent,
	@location,
	@date,
	@total_cases,
	@new_cases,
	@new_cases_smoothed,
	@total_deaths,
	@new_deaths,
	@new_deaths_smoothed,
	@total_cases_per_million,
	@new_cases_per_million,
	@new_cases_smoothed_per_million,
	@total_deaths_per_million,
	@new_deaths_per_million,
	@new_deaths_smoothed_per_million,
	@reproduction_rate,
	@icu_patients,
	@icu_patients_per_million,
	@hosp_patients,
	@hosp_patients_per_million,
	@weekly_icu_admissions,
	@weekly_icu_admissions_per_million,
	@weekly_hosp_admissions,
	@weekly_hosp_admissions_per_million,
	@new_tests,
	@total_tests,
	@total_tests_per_thousand,
	@new_tests_per_thousand,
	@new_tests_smoothed,
	@new_tests_smoothed_per_thousand,
	@positive_rate,
	@tests_per_case,
	@tests_units,
	@total_vaccinations,
	@people_vaccinated,
	@people_fully_vaccinated,
	@new_vaccinations,
	@new_vaccinations_smoothed,
	@total_vaccinations_per_hundred,
	@people_vaccinated_per_hundred,
	@people_fully_vaccinated_per_hundred,
	@new_vaccinations_smoothed_per_million,
	@stringency_index,
	@population,
	@population_density,
	@median_age,
	@aged_65_older,
	@aged_70_older,
	@gdp_per_capita,
	@extreme_poverty,
	@cardiovasc_death_rate,
	@diabetes_prevalence,
	@female_smokers,
	@male_smokers,
	@handwashing_facilities,
	@hospital_beds_per_thousand,
	@life_expectancy,
	@human_development_index
)
SET 
	iso_code = NULLIF(@iso_code, ''),
	continent = NULLIF(@continent, ''),
	location = NULLIF(@location, ''),
	date = STR_TO_DATE(@date, '%Y-%m-%d'), -- auf richtige date format achten
	total_cases = NULLIF(@total_cases, ''),
	new_cases = NULLIF(@new_cases, ''),
	new_cases_smoothed = NULLIF(@new_cases_smoothed, ''),
	total_deaths = NULLIF(@total_deaths, ''),
	new_deaths = NULLIF(@new_deaths, ''),
	new_deaths_smoothed = NULLIF(@new_deaths_smoothed, ''),
	total_cases_per_million = NULLIF(@total_cases_per_million, ''),
	new_cases_per_million = NULLIF(@new_cases_per_million, ''),
	new_cases_smoothed_per_million = NULLIF(@new_cases_smoothed_per_million, ''),
	total_deaths_per_million = NULLIF(@total_deaths_per_million, ''),
	new_deaths_per_million = NULLIF(@new_deaths_per_million, ''),
	new_deaths_smoothed_per_million = NULLIF(@new_deaths_smoothed_per_million, ''),
	reproduction_rate = NULLIF(@reproduction_rate, ''),
	icu_patients = NULLIF(@icu_patients, ''),
	icu_patients_per_million = NULLIF(@icu_patients_per_million, ''),
	hosp_patients = NULLIF(@hosp_patients, ''),
	hosp_patients_per_million = NULLIF(@hosp_patients_per_million, ''),
	weekly_icu_admissions = NULLIF(@weekly_icu_admissions, ''),
	weekly_icu_admissions_per_million = NULLIF(@weekly_icu_admissions_per_million, ''),
	weekly_hosp_admissions = NULLIF(@weekly_hosp_admissions, ''),
	weekly_hosp_admissions_per_million = NULLIF(@weekly_hosp_admissions_per_million, ''),
	new_tests = NULLIF(@new_tests, ''),
	total_tests = NULLIF(@total_tests, ''),
	total_tests_per_thousand = NULLIF(@total_tests_per_thousand, ''),
	new_tests_per_thousand = NULLIF(@new_tests_per_thousand, ''),
	new_tests_smoothed = NULLIF(@new_tests_smoothed, ''),
	new_tests_smoothed_per_thousand = NULLIF(@new_tests_smoothed_per_thousand, ''),
	positive_rate = NULLIF(@positive_rate, ''),
	tests_per_case = NULLIF(@tests_per_case, ''),
	tests_units = NULLIF(@tests_units, ''),
	total_vaccinations = NULLIF(@total_vaccinations, ''),
	people_vaccinated = NULLIF(@people_vaccinated, ''),
	people_fully_vaccinated = NULLIF(@people_fully_vaccinated, ''),
	new_vaccinations = NULLIF(@new_vaccinations, ''),
	new_vaccinations_smoothed = NULLIF(@new_vaccinations_smoothed, ''),
	total_vaccinations_per_hundred = NULLIF(@total_vaccinations_per_hundred, ''),
	people_vaccinated_per_hundred = NULLIF(@people_vaccinated_per_hundred, ''),
	people_fully_vaccinated_per_hundred = NULLIF(@people_fully_vaccinated_per_hundred, ''),
	new_vaccinations_smoothed_per_million = NULLIF(@new_vaccinations_smoothed_per_million, ''),
	stringency_index = NULLIF(@stringency_index, ''),
	population = NULLIF(@population, ''),
	population_density = NULLIF(@population_density, ''),
	median_age = NULLIF(@median_age, ''),
	aged_65_older = NULLIF(@aged_65_older, ''),
	aged_70_older = NULLIF(@aged_70_older, ''),
	gdp_per_capita = NULLIF(@gdp_per_capita, ''),
	extreme_poverty = NULLIF(@extreme_poverty, ''),
	cardiovasc_death_rate = NULLIF(@cardiovasc_death_rate, ''),
	diabetes_prevalence = NULLIF(@diabetes_prevalence, ''),
	female_smokers = NULLIF(@female_smokers, ''),
	male_smokers = NULLIF(@male_smokers, ''),
	handwashing_facilities = NULLIF(@handwashing_facilities, ''),
	hospital_beds_per_thousand = NULLIF(@hospital_beds_per_thousand, ''),
	life_expectancy = NULLIF(@life_expectancy, ''),
	human_development_index = NULLIF(@human_development_index, '');

-- Jetzt durchläuft die zweite Tabelle covidvaccinations dieselben Prozesse wie die erste Tabelle coviddeaths.
DROP TABLE IF EXISTS covidvaccinations;
CREATE TABLE covidvaccinations (
	iso_code VARCHAR(10),
	continent VARCHAR(50),
	location VARCHAR(100),
	date DATE,
	new_tests INT,
	total_tests INT,
	total_tests_per_thousand DOUBLE,
	new_tests_per_thousand DOUBLE,
	new_tests_smoothed INT,
	new_tests_smoothed_per_thousand DOUBLE,
	positive_rate DOUBLE,
	tests_per_case DOUBLE,
	tests_units VARCHAR(50),
	total_vaccinations INT,
	people_vaccinated INT,
	people_fully_vaccinated INT,
	new_vaccinations INT,
	new_vaccinations_smoothed INT,
	total_vaccinations_per_hundred DOUBLE,
	people_vaccinated_per_hundred DOUBLE,
	people_fully_vaccinated_per_hundred DOUBLE,
	new_vaccinations_smoothed_per_million DOUBLE,
	stringency_index DOUBLE,
	population_density DOUBLE,
	median_age DOUBLE,
	aged_65_older DOUBLE,
	aged_70_older DOUBLE,
	gdp_per_capita DOUBLE,
	extreme_poverty DOUBLE,
	cardiovasc_death_rate DOUBLE,
	diabetes_prevalence DOUBLE,
	female_smokers DOUBLE,
	male_smokers DOUBLE,
	handwashing_facilities DOUBLE,
	hospital_beds_per_thousand DOUBLE,
	life_expectancy DOUBLE,
	human_development_index DOUBLE
);

LOAD DATA INFILE 'path\\CovidVaccinations.csv'
INTO TABLE covidvaccinations
FIELDS TERMINATED BY ';' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
	@iso_code,
	@continent,
	@location,
	@date, 
	@new_tests,
	@total_tests,
	@total_tests_per_thousand,
	@new_tests_per_thousand,
	@new_tests_smoothed,
	@new_tests_smoothed_per_thousand,
	@positive_rate,
	@tests_per_case,
	tests_units,
	@total_vaccinations,
	@people_vaccinated,
	@people_fully_vaccinated,
	@new_vaccinations,
	@new_vaccinations_smoothed,
	@total_vaccinations_per_hundred,
	@people_vaccinated_per_hundred,
	@people_fully_vaccinated_per_hundred,
	@new_vaccinations_smoothed_per_million,
	@stringency_index,
	@population_density,
	@median_age,
	@aged_65_older,
	@aged_70_older,
	@gdp_per_capita,
	@extreme_poverty,
	@cardiovasc_death_rate,
	@diabetes_prevalence,
	@female_smokers,
	@male_smokers,
	@handwashing_facilities,
	@hospital_beds_per_thousand,
	@life_expectancy,
	@human_development_index
)
SET 
	iso_code = NULLIF(@iso_code, ''),
	continent = NULLIF(@continent, ''),
	location = NULLIF(@location, ''),
	date = STR_TO_DATE(@date, '%Y-%m-%d'), -- auch hier auf richtige date format achten
	new_tests = NULLIF(@new_tests, ''),
	total_tests = NULLIF(@total_tests, ''),
	total_tests_per_thousand = NULLIF(@total_tests_per_thousand, ''),
	new_tests_per_thousand = NULLIF(@new_tests_per_thousand, ''),
	new_tests_smoothed = NULLIF(@new_tests_smoothed, ''),
	new_tests_smoothed_per_thousand = NULLIF(@new_tests_smoothed_per_thousand, ''),
	positive_rate = NULLIF(@positive_rate, ''),
	tests_per_case = NULLIF(@tests_per_case, ''),
	total_vaccinations = NULLIF(@total_vaccinations, ''),
	people_vaccinated = NULLIF(@people_vaccinated, ''),
	people_fully_vaccinated = NULLIF(@people_fully_vaccinated, ''),
	new_vaccinations = NULLIF(@new_vaccinations, ''),
	new_vaccinations_smoothed = NULLIF(@new_vaccinations_smoothed, ''),
	total_vaccinations_per_hundred = NULLIF(@total_vaccinations_per_hundred, ''),
	people_vaccinated_per_hundred = NULLIF(@people_vaccinated_per_hundred, ''),
	people_fully_vaccinated_per_hundred = NULLIF(@people_fully_vaccinated_per_hundred, ''),
	new_vaccinations_smoothed_per_million = NULLIF(@new_vaccinations_smoothed_per_million, ''),
	stringency_index = NULLIF(@stringency_index, ''),
	population_density = NULLIF(@population_density, ''),
	median_age = NULLIF(@median_age, ''),
	aged_65_older = NULLIF(@aged_65_older, ''),
	aged_70_older = NULLIF(@aged_70_older, ''),
	gdp_per_capita = NULLIF(@gdp_per_capita, ''),
	extreme_poverty = NULLIF(@extreme_poverty, ''),
	cardiovasc_death_rate = NULLIF(@cardiovasc_death_rate, ''),
	diabetes_prevalence = NULLIF(@diabetes_prevalence, ''),
	female_smokers = NULLIF(@female_smokers, ''),
	male_smokers = NULLIF(@male_smokers, ''),
	handwashing_facilities = NULLIF(@handwashing_facilities, ''),
	hospital_beds_per_thousand = NULLIF(@hospital_beds_per_thousand, ''),
	life_expectancy = NULLIF(@life_expectancy, ''),
	human_development_index = NULLIF(@human_development_index, '');

-- Ich schaue mir die Tabellen an und überprüfe, ob alles korrekt ist und behebe die Fehler, falls nötig
select * from coviddeaths;
select * from covidvaccinations;

/*
Ich schaue mir die Daten über Covid19 erstmal an.
Vorab ist zu bemerken, dass diese Datensätze unvollständig sind. Z.B. haben nicht alle Länder ihre Bevölkerungszahl, Infektionen, Tode, usw. angegeben. Das sehe ich anhand der NULLs in den Zellen.
Da diese Tabellen riesige Datenmengen enthalten, eeignet es sich gut als repräsentative Statistik und Auswertung.
Ich habe vor, die Tabellen coviddeaths und covidvaccinations anzuschauen. Den Anfang macht die Tabelle coviddeaths.
*/
SELECT
    location AS Land,
    DATE,
    new_cases AS Infektionen_pro_Tag,
	total_cases AS Impfungen_kumuliert,
    new_deaths AS Tote_pro_Tag,
    total_deaths AS Tote_kumuliert,
    formatWithThousandsSeparator(population) AS Bevölkerungszahl
FROM
    coviddeaths 
WHERE
    continent IS NOT NULL 
    AND location = 'Germany'
ORDER BY
    location,
    DATE;
    
/*
Es ist zu beachten, dass meine Daten nur bis zum 30. April 2021 up-to-date sind. Aktueller Stand ist work in progress.
*/
SELECT 
	DISTINCT date
FROM
    coviddeaths
ORDER BY 
	date DESC;

/*
Zum Überblick schaue ich mir die Infektionen und Tode in Deutschland an.
Seit 27. Januar 2020 wurden Infektionen ermittelt und die Fälle steigen von Tag zu Tag immer mehr.
Seit 09. März wurden Tode ermittelt und auch diese steigen.
Was mir aufgefallen ist, dass die Tode pro Infektionen immer schwankt. Ich kann einiges daraus herleiten. 
Erstens, im Sommer beginnend mit Juni steigen die Todeszahlen langsamer als die Infektionen, sodass die Wahrscheinlich sinkt und es bis zum Winter diesen Trend beibehält.
Das ändert sich jedoch im Winter beginnend mit Dezember. Die Wahrscheinlichkeit steigt wieder bis zum Frühling 2021. 
Dann sinkt es wieder, was auf die Einführung der Impfung gegen Covid19 seit Ende Dezember 2020 zurückzuführen sein könnte.
*/
SELECT 
    location AS Land,
    DATE,
    total_cases AS Infektionen_kumuliert,
    total_deaths AS Tote_kumuliert,
    CONCAT(ROUND(total_deaths / total_cases * 100, 2), '%') AS Tote_pro_Infizierten
FROM
    coviddeaths
WHERE
    location = 'Germany'
        AND continent IS NOT NULL
ORDER BY 
	DATE;

/*
Am 11. Juni 2020 war der Höhepunkt mit der höchsten Tode pro Infektionen und auch sonst sind die Top25 überwiegend im Juni und teilweise im Mai zu sehen.
*/
SELECT
    location AS Land,
    DATE,
    total_cases AS Impfungen_kumuliert,
    total_deaths AS Tote_kumuliert,
    CONCAT(ROUND((total_deaths / total_cases) * 100, 2), '%') AS Tote_pro_Infizierten,
    ROW_NUMBER() OVER (ORDER BY (total_deaths / total_cases) DESC) AS Ranking
FROM
    coviddeaths 
WHERE
    location = 'Germany' 
    AND continent IS NOT NULL;

/*
Ich schaue mir die Ansteckungsrate an, also die Infektion gemessen an der Bevölkerungszahl in Deutschland.
Von Tag zu Tag erhöhen sich die Infektionen pro Einwohner. Das macht Sinn, da die Bevölkerungszahl gleich bleibt, die Infektionen jedoch weiter steigen.
*/
SELECT
    location AS Land,
    DATE,
    population AS Bevölkerungszahl,
    total_cases AS Infektionen_kumuliert,
    CONCAT(ROUND((total_cases / population) * 100, 2), '%') AS Infektionen_pro_Einwohner 
FROM
    coviddeaths 
WHERE
    location = 'Germany' 
    AND continent IS NOT NULL 
ORDER BY
    DATE;

/*
Am 30. April 2021 war der Höhepunkt mit den höchsten Infektionen pro Einwohner. 
*/
SELECT
    location AS Land,
    DATE,
    population AS Bevölkerungszahl,
    total_cases AS Infektionen_kumuliert,
    CONCAT(ROUND((total_cases / population) * 100, 2), '%') AS Infektionen_pro_Einwohner,
    ROW_NUMBER() OVER (ORDER BY (total_cases / population) DESC) AS Ranking 
FROM
    coviddeaths 
WHERE
    location = 'Germany' 
    AND continent IS NOT NULL;

/*
Bevor ich Ranking-System einführe, will ich die Anzahl der Länder wissen. Es sind 210 Länder.
*/
SELECT 
    COUNT(DISTINCT location) AS Anzahl_Länder
FROM
    coviddeaths
WHERE
    continent IS NOT NULL;

/*
Die Top3-Länder mit den höchsten Infektionen pro Einwohner sind Andorra, Montenegro und Tschechien. USA ist auf dem 9. Platz von 210 Ländern.
*/
SELECT
    location AS Land,
    population AS Bevölkerungszahl,
    MAX(total_cases) AS Summe_der_Infektionen,
    CONCAT(ROUND(MAX(total_cases / population) * 100, 2), '%') AS Infektionen_pro_Einwohner,
    ROW_NUMBER() OVER (ORDER BY MAX(total_cases / population) DESC) AS Ranking 
FROM
    coviddeaths 
WHERE
    continent IS NOT NULL 
GROUP BY
    location;

-- Deutschland ist auf Platz 62 von 210 Ländern den höchsten Infektionen pro Einwohner. Mit CTE kann ich auf das Land Deutschland eingrenzen.
WITH ranked_data AS 
(
    SELECT
        location AS Land,
        population AS Bevölkerungszahl,
        MAX(total_cases) AS Summe_der_Infektionen,
		CONCAT(ROUND(MAX(total_cases / population) * 100, 2), '%') AS Infektionen_pro_Einwohner,
        ROW_NUMBER() OVER (ORDER BY MAX(total_cases / population) DESC) AS Ranking 
    FROM
        coviddeaths 
    WHERE
        continent IS NOT NULL 
    GROUP BY
        location,
        population 
)
SELECT
    Land,
    Bevölkerungszahl,
    Summe_der_Infektionen,
    Infektionen_pro_Einwohner,
    Ranking 
FROM
    ranked_data 
WHERE
    Land = 'Germany';

-- Top3-Länder mit den höchsten Tode pro Einwohner sind Ungarn, Tschechien und San Marino. UK ist auf dem 14. Platz von 210 Ländern.
SELECT
	location AS Land,
	population AS Bevölkerungszahl,
	MAX(total_deaths) AS Summe_der_Toten,
	CONCAT(ROUND(MAX(total_deaths / population) * 100, 2), '%') AS Tote_pro_Einwohner,
	ROW_NUMBER() OVER (ORDER BY MAX(total_deaths / population) DESC) AS Ranking
FROM
	coviddeaths
WHERE
	continent IS NOT NULL
GROUP BY
	location;
    
-- Deutschland ist auf Platz 43 von 210 Ländern den höchsten Tode pro Einwohner.
WITH ranked_data AS 
(
    SELECT
        location AS Land,
        population AS Bevölkerungszahl,
        MAX(total_deaths) AS Summe_der_Toten,
		CONCAT(ROUND(MAX(total_deaths / population) * 100, 2), '%') AS Tote_pro_Einwohner,
        ROW_NUMBER() OVER (ORDER BY MAX(total_deaths / population) DESC) AS Ranking 
    FROM
        coviddeaths 
    WHERE
        continent IS NOT NULL 
    GROUP BY
        location,
        population 
)
SELECT
    Land,
    Bevölkerungszahl,
    Summe_der_Toten,
    Tote_pro_Einwohner,
    Ranking 
FROM
    ranked_data 
WHERE
    Land = 'Germany';

-- Von allen Kontinenten ist Nordamerika auf 1. Platz mit der höchsten Summe der Tode
SELECT
    continent AS Kontinent,
    MAX(total_deaths) AS Summe_der_Toten,
	ROW_NUMBER() OVER (ORDER BY MAX(total_deaths) DESC) AS Ranking 
FROM
    coviddeaths 
WHERE
    continent IS NOT NULL 
GROUP BY
    continent;
    
-- Wenn man jedoch auf die höchsten Tode pro Einwohner schaut, dann liegt Europa auf dem 1. Platz
SELECT
	continent AS Kontinent,
	population AS Bevölkerungszahl,
	MAX(total_deaths) AS Summe_der_Toten,
	CONCAT(ROUND(MAX(total_deaths / population) * 100, 2), '%') AS Tote_pro_Einwohner,
	ROW_NUMBER() OVER (ORDER BY MAX(total_deaths / population) DESC) AS Ranking
FROM 
	coviddeaths
WHERE
	continent IS NOT NULL
GROUP BY
	continent;

/*
Todesrate weltweit pro Tag. Hier wurde nach der höchsten Tode sortiert. 24. Februar 2020 in Afghanistan wurde die höchste Todeszahl der bereits Infizierten ermittelt.
Was mir aufgefallen ist, dass innerhalb des Zeitraums von 486 Tagen Hauptsächlich Afghanistan Rekorde der am höchsten Tode zu verzeichnen hatte. Argentinien kommt teilweise in der Liste vor.
*/
SELECT
    DATE,
    location AS Land,
    SUM(new_cases) AS Infektionen_pro_Tag,
    SUM(new_deaths) AS Tote_pro_Tag,
	CONCAT(ROUND(SUM(new_deaths) / SUM(new_cases) * 100, 2), '%') AS Tote_von_Infizierten_pro_Tag,
	ROW_NUMBER() OVER (ORDER BY SUM(new_deaths) / SUM(new_cases) DESC) AS Ranking
FROM
    coviddeaths 
WHERE
    continent IS NOT NULL 
GROUP BY
    DATE;
    
-- Insgesamt wurden 486 Tage aufgelistet
SELECT
	COUNT(DISTINCT DATE)
FROM 
	coviddeaths;

-- Eine Übersich über Infektionen insgesamt, Tode insgesamt und Tode pro Infizierten
SELECT
	formatWithThousandsSeparator(SUM(new_cases)) AS Infektionen_insgesamt,
	formatWithThousandsSeparator(SUM(new_deaths)) AS Tote_insgesamt,
    CONCAT(ROUND(SUM(new_deaths) / SUM(new_cases) * 100, 2), '%') AS Tote_von_Infizierten 
FROM
    coviddeaths 
WHERE
    continent IS NOT NULL 
ORDER BY
    Infektionen_insgesamt,
    Tote_insgesamt;
    
/*
-- Randinfo für Tausender-Trennung
-- Set the log_bin_trust_function_creators variable to 1 (if not already set)
SET GLOBAL log_bin_trust_function_creators = 1;

-- Create a user-defined function for thousand-separator
DELIMITER //
CREATE FUNCTION formatWithThousandsSeparator(number INT) RETURNS VARCHAR(255)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE formatted VARCHAR(255);
    SET formatted = FORMAT(number, 0);
    RETURN formatted;
END;
//
DELIMITER ;

-- Use the custom CONCAT function in the SELECT statement
SELECT
    formatWithThousandsSeparator(SUM(new_cases)) AS 'Infektionen insgesamt',
    formatWithThousandsSeparator(SUM(new_deaths)) AS 'Tode insgesamt',
    CONCAT(ROUND(SUM(new_deaths) / SUM(new_cases) * 100, 2), '%') AS 'Tode pro Infizierten'
FROM
    coviddeaths 
WHERE
    continent IS NOT NULL;
*/

-- Wie hoch ist die Impfungsrate der Bevölkerung? Dazu join mit zwei Tabellen
SELECT
    dea.continent AS Kontinent,
    dea.location AS Land,
    dea.DATE,
    dea.population AS Bevölkerungszahl,
    vac.new_vaccinations AS Impfungen_pro_Tag,
    SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.DATE) AS Impfungen_kumuliert	-- kumuliert alle Impfungen zusammen, partition by kumuliert pro Länder, ohne partition by dea.location würde es einfach alle länder zusammen addieren, order by nicht vergessen, sonst zeigt es nur sum auf alle Zeilen an
    -- , (Kumulierte_Impfungen/population)*100 funktioniert ohne CTE oder temp_tables nicht
FROM
    coviddeaths AS dea 
    JOIN
        covidvaccinations AS vac 
        ON dea.location = vac.location 
        AND dea.DATE = vac.DATE 
WHERE
    dea.continent IS NOT NULL 
ORDER BY
    dea.location,
    dea.DATE;

/* 
Mit CTE können Kalkulationen durchgeführt werden
Erst am 28. Dezember 2020 wurden die Impfungen in Deutschland eingeführt. Interessant ist, dass der Impfstoff ungefähr 1 Jahr nach dem Ausbruch der Covid19-Pandemie entwickelt und an Menschen verabreicht wurde.
Stand 29. April 2021 wurden 34% der Einwohner in Deutschland geimpft.
*/
WITH PopvsVac 
(
	Kontinent, 
    Land, 
    DATE, 
    Bevölkerungszahl, 
    Impfungen_pro_Tag,
    Impfungen_kumuliert
) -- die Anzahl und Name der Spalten müssen hier oben mit den unten übereinstimmen, sonst error
AS 
(
    SELECT
        dea.continent AS Kontinent,
        dea.location AS Land,
        dea.DATE,
        dea.population AS Bevölkerungszahl,
        vac.new_vaccinations AS Impfungen_pro_Tag,
        SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.DATE) AS Impfungen_kumuliert 		-- die Anzahl und Name der Spalten müssen hier unten mit den oben übereinstimmen, sonst error
    FROM
        coviddeaths AS dea 
        JOIN
            covidvaccinations AS vac 
            ON dea.location = vac.location 
            AND dea.DATE = vac.DATE 
    WHERE
        dea.continent IS NOT NULL
)
SELECT
    *,
    CONCAT(ROUND(Impfungen_kumuliert / Bevölkerungszahl * 100, 2), '%') AS Impfungen_pro_Einwohner
FROM
    PopvsVac
WHERE
	Land = 'Germany';

-- Die gleiche Ausgabe, aber mit temp_tables 
DROP temporary TABLE IF EXISTS temp_PercentPopulationVaccinated;
CREATE temporary TABLE temp_PercentPopulationVaccinated 
(
	Kontinent VARCHAR(50), 
    Land VARCHAR(100), 
    DATE DATE, 
    Bevölkerungszahl BIGINT, 
    Impfungen_pro_Tag INT, 
    Impfungen_kumuliert INT
);

INSERT INTO
    temp_PercentPopulationVaccinated 
    SELECT
        dea.continent AS Kontinent,
        dea.location AS Land,
        dea.DATE,
        dea.population AS Bevölkerungszahl,
        vac.new_vaccinations AS Impfungen_pro_Tag,
        SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.DATE) AS Impfungen_kumuliert
    FROM
        coviddeaths AS dea 
        JOIN
            covidvaccinations AS vac 
            ON dea.location = vac.location 
            AND dea.DATE = vac.DATE;

SELECT
    *,
    CONCAT(ROUND(Impfungen_kumuliert / Bevölkerungszahl * 100, 2), '%') AS Impfungen_pro_Einwohner
FROM
    temp_PercentPopulationVaccinated
WHERE
	Land = 'Germany';

-- View erstellen um Daten für Visualisierung zu speichern
DROP VIEW IF EXISTS PercentPopulationVaccinated;
CREATE VIEW PercentPopulationVaccinated AS 
SELECT
    dea.continent AS Kontinent,
    dea.location AS Land,
    dea.DATE,
    dea.population AS Bevölkerungszahl,
    vac.new_vaccinations Impfungen_pro_Tag,
    SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.DATE) AS Impfungen_kumuliert
FROM
    coviddeaths AS dea 
    JOIN
        covidvaccinations AS vac 
        ON dea.location = vac.location 
        AND dea.DATE = vac.DATE 
WHERE
    dea.continent IS NOT NULL;
    
SELECT
    * 
FROM
    PercentPopulationVaccinated;
