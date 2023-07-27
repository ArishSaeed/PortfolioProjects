/*
Für Data Cleaning nehme ich die Datensätze über Immobilien.
Die Datei werde ich ebenfalls auf github hochladen.
Folgendes muss in den CSV Dateien abgeändert werden, bevor sie importiert werden können, sonst entstehen errors:
Dezimal-Separator auf "." und Tausender-Separator auf "," einstellen
Ebenso muss Datum auf "JJJJ-MM-TT" umgeändert werden

Tabellen mit header dienen als Grundgerüst, das später mit Daten gefüllt wird
Auf genaue Schreibweise, Reihenfolge der Spalten und ihre Datentypen achten
*/
-- Ich erstelle neue Tabelle mit header. Es dient als Grundgerüst, das später mit Daten gefüllt wird
DROP TABLE IF EXISTS NashvilleHousing;
CREATE TABLE NashvilleHousing 
(
	UniqueID INT,
	ParcelID VARCHAR(255),
	LandUse	VARCHAR(255),
	PropertyAddress	VARCHAR(255),
	SaleDate DATE,
	SalePrice VARCHAR(255),
	LegalReference VARCHAR(255),
	SoldAsVacant VARCHAR(255),
	OwnerName VARCHAR(255),
	OwnerAddress VARCHAR(255),
	Acreage	DOUBLE,
	TaxDistrict	VARCHAR(255),
	LandValue INT,
	BuildingValue INT,
	TotalValue INT,
	YearBuilt INT,
	Bedrooms INT,
	FullBath INT,
	HalfBath INT
);

-- Ich importiere die Daten von csv file und befülle die leere Tabelle mit Daten
LOAD DATA INFILE 'path\\Nashville_Housing_Data.csv'
INTO TABLE NashvilleHousing
FIELDS TERMINATED BY ';' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
( -- Normalerweise würde load data infile die Daten importieren und damit wäre der Prozess fertig. Jedoch führe ich noch zusätzlich NULL-Handling durch, damit keine errors entstehen. Schließlich brauche ich NULLs für meine Analysen.
	@UniqueID, 
	@ParcelID, 
	@LandUse, 
	@PropertyAddress, 
	@SaleDate, 
	@SalePrice, 
	@LegalReference, 
	@SoldAsVacant, 
	@OwnerName, 
	@OwnerAddress, 
	@Acreage, 
	@TaxDistrict, 
	@LandValue, 
	@BuildingValue, 
	@TotalValue, 
	@YearBuilt, 
	@Bedrooms, 
	@FullBath, 
	@HalfBath
)
SET
	UniqueID = NULLIF(@UniqueID, ''),
	ParcelID = NULLIF(@ParcelID, ''),
	LandUse	= NULLIF(@LandUse, ''),
	PropertyAddress = NULLIF(@PropertyAddress, ''),
	SaleDate = STR_TO_DATE(@SaleDate, '%Y-%m-%d'), -- Das erspart mir später die Konvertierung von DATETIME auf DATE und es gewährleistet, dass ich die gewünschte date-format habe
	SalePrice = NULLIF(@SalePrice, ''),
	LegalReference = NULLIF(@LegalReference, ''),
	SoldAsVacant = NULLIF(@SoldAsVacant, ''),
	OwnerName = NULLIF(@OwnerName, ''),
	OwnerAddress = NULLIF(@OwnerAddress, ''),
	Acreage = NULLIF(@Acreage, ''),
	TaxDistrict = NULLIF(@TaxDistrict, ''),
	LandValue = NULLIF(@LandValue, ''),
	BuildingValue = NULLIF(@BuildingValue, ''),
	TotalValue = NULLIF(@TotalValue, ''),
	YearBuilt = NULLIF(@YearBuilt, ''),
	Bedrooms = NULLIF(@Bedrooms, ''),
	FullBath = NULLIF(@FullBath, ''),
	HalfBath = NULLIF(@HalfBath, '');

/* 
Data Cleaning in SQL ist notwendig, um die Daten für spätere Kalkulation oder Analysen nutzbar zu machen.
Erstmal eine Übersicht über die Tabelle
*/
SELECT 
	*
FROM 
	nashvillehousing;
----------------------------------------------------------------------------------
/*
-- Optional, da ich  bereits beim Importieren der Daten von csv file auf die Date-Format konvertiert habe
-- Ich erstelle neue Spalte SaleDateConverted
ALTER TABLE nashvillehousing ADD SaleDateConverted DATE AFTER SaleDate;

-- Ich fülle die neue Spalte mit den Daten von der alten Spalte mit dem neuen Date-Format
UPDATE nashvillehousing SET SaleDateConverted = DATE(SaleDate);

-- Jetzt lösche ich die nicht benötigte Spalte
ALTER TABLE nashvillehousing DROP COLUMN SaleDate;
*/
----------------------------------------------------------------------------------
-- PropertyAddress mit NULL sollen mit Daten ersetzt werden. ParcelID ist dem PropertyAddress zuzuordnen.
SELECT
	*
FROM
    nashvillehousing
-- WHERE
	-- PropertyAddress IS NULL
ORDER BY ParcelID;

/*
Hier sehe ich die Daten, die ich für UPDATE nutzen kann
Mit JOIN kombiniere ich die Tabelle mit sich selbst, wo beide ParcelID gleich sind, aber UniqueID nicht, also nicht dieselbe Zeile
Nach dem UPDATE wird dieser code nichts mehr ausgeben, was bedeutet, dass der UPDATE funktioniert hat und die NULL in Tabelle a durch die Daten von Tabelle b ersetzt wurden.
*/
SELECT
	a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, IFNULL(a.PropertyAddress, b.PropertyAddress)
FROM
    nashvillehousing a
JOIN
	nashvillehousing b
    ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;

-- Mit IFNULL ersetze ich die Tabelle a mit den Werten von Tabelle b
UPDATE
    nashvillehousing a 
JOIN
	nashvillehousing b 
	ON a.ParcelID = b.ParcelID 
	AND a.UniqueID <> b.UniqueID 
SET
    a.PropertyAddress = IFNULL(a.PropertyAddress, b.PropertyAddress) 
WHERE
    a.PropertyAddress IS NULL;
----------------------------------------------------------------------------------
/*
Normalisierung der Adresse, also eine Spalte auf zwei Spalten aufteilen (Adresse und Stadt)
Ich werde die Spalte PropertyAddress bearbeiten
*/
SELECT
	PropertyAddress
FROM
    nashvillehousing;
    
-- In der Spalte PropertyAddress sind Adresse und Stadt zusammen eingetragen. Mit SUBSTRING_INDEX() trenne ich die beiden voneinander und trage sie in zwei separate Spalten ein. Der Komma dient als Trennzeichen.
SELECT
    SUBSTRING_INDEX(PropertyAddress, ',', 1) AS Address, -- Der erste Teil (Adresse) der Zeichenfolge bis zum Komma wird extrahiert, jedoch ohne Komma
    SUBSTRING_INDEX(PropertyAddress, ',', -1) AS City -- Der letzte Teil (Stadt) der Zeichenfolge nach dem Komma wird extrahiert, wieder ohne Komma
FROM
    nashvillehousing;
    
/*
Jetzt gehe ich einen Schritt weiter und entferne unnötige Leerzeichen mit TRIM(). Ich packe den Code von oben innerhalb von TRIM().
Vor dem Update will ich mit select die Ausgabe auf seine Richtigkeit überprüfen
*/
SELECT
    TRIM(SUBSTRING_INDEX(PropertyAddress, ',', 1)) AS PropertyAddress_new,
    TRIM(SUBSTRING_INDEX(PropertyAddress, ',', -1)) AS PropertyCity_new
FROM
    nashvillehousing;
   
 
-- Erstmal überprüfe ich, ob die Spalten PropertyAddress_new und PropertyCity_new schon vorhanden sind.
SHOW COLUMNS FROM nashvillehousing LIKE '%propertyaddress%';
SHOW COLUMNS FROM nashvillehousing LIKE '%propertycity%';

/*
Wenn die Spalten noch nicht vorhanden sind, dann füge ich sie hinzu.
Ich erstelle zwei neue Spalten Adresse und Stadt
*/
ALTER TABLE nashvillehousing
ADD COLUMN PropertyAddress_new VARCHAR(255) AFTER PropertyAddress,
ADD COLUMN PropertyCity_new VARCHAR(255) AFTER PropertyAddress_new;

-- Ich befülle die neuen Spalten mit Daten von der alten Spalte
UPDATE 
	nashvillehousing
SET
    PropertyAddress_new = TRIM(SUBSTRING_INDEX(PropertyAddress, ',', 1)),
    PropertyCity_new = TRIM(SUBSTRING_INDEX(PropertyAddress, ',', -1));

-- Jetzt sehe ich zwei neue Spalten.
SELECT
	*
FROM
	nashvillehousing;

-- Ich werde die Spalte OwnerAddress bearbeiten, hier brauche ich drei Spalten (Adresse, Stadt, Land)
SELECT
	OwnerAddress
FROM
    nashvillehousing;

-- Vor dem Update will ich mit select die Ausgabe auf seine Richtigkeit überprüfen
SELECT
	TRIM(SUBSTRING_INDEX(OwnerAddress, ',', 1)) AS OwnerAddress_new,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 2), ',', -1)) AS OwnerCity_new,
    TRIM(SUBSTRING_INDEX(OwnerAddress, ',', -1)) AS OwnerState_new
FROM 
	nashvillehousing;

-- Erstmal überprüfe ich, ob die Spalten OwnerAddress_new, OwnerCity_new und OwnerState_new schon vorhanden sind.
SHOW COLUMNS FROM nashvillehousing LIKE '%owneraddress%'; -- noch überprüfen
SHOW COLUMNS FROM nashvillehousing LIKE '%ownercity%';
SHOW COLUMNS FROM nashvillehousing LIKE '%ownerstate%';

/*
Wenn die Spalten noch nicht vorhanden sind, dann füge ich sie hinzu.
Ich erstelle drei neue Spalten Adresse, Stadt und Land
*/
ALTER TABLE nashvillehousing
ADD COLUMN OwnerAddress_new VARCHAR(255) AFTER OwnerAddress,
ADD COLUMN OwnerCity_new VARCHAR(255) AFTER OwnerAddress_new,
ADD COLUMN OwnerState_new VARCHAR(255) AFTER OwnerCity_new;

-- Ich befülle die neuen Spalten mit Daten von der alten Spalte
UPDATE 
	nashvillehousing
SET
    OwnerAddress_new = TRIM(SUBSTRING_INDEX(OwnerAddress, ',', 1)),
    OwnerCity_new = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 2), ',', -1)),
    OwnerState_new = TRIM(SUBSTRING_INDEX(OwnerAddress, ',', -1));

-- Jetzt sehe ich drei neue Spalten.
SELECT
	*
FROM
    nashvillehousing;
----------------------------------------------------------------------------------

/*
Die Spalte SoldAsVacant hat vier verschiedene Werte. Da Yes und No öfters vorkommen, werde ich die Y und N dementsprechen abändern.
Nach dem Update sind nur noch Yes und No zu sehen
*/
SELECT 
	DISTINCT SoldAsVacant,
    COUNT(SoldAsVacant) 
FROM
    nashvillehousing 
GROUP BY
    SoldAsVacant 
ORDER BY
    COUNT(SoldAsVacant);

-- Vor dem Update will ich mit select die Ausgabe auf seine Richtigkeit überprüfen
SELECT
    SoldAsVacant,
    CASE
        WHEN
            SoldAsVacant = 'Y' 
        THEN
            'Yes' 
        WHEN
            SoldAsVacant = 'N' 
        THEN
            'No' 
        ELSE
            SoldAsVacant 
    END AS VacantStatus
FROM
    nashvillehousing;
    
-- Mit dem Update werden alle Y und No durch Yes und No ersetzt.
UPDATE
    nashvillehousing 
SET
    SoldAsVacant = 
    CASE
        WHEN
            SoldAsVacant = 'Y' 
        THEN
            'Yes' 
        WHEN
            SoldAsVacant = 'N' 
        THEN
            'No' 
        ELSE
            SoldAsVacant 
    END;
----------------------------------------------------------------------------------
/*
Duplikate entfernen
Normalerweise könnte man anhand des UniqueID sehen, ob es Duplikate gibt. Jedoch funktioniert das hier nicht, da Duplikate mit verschiedenen UniqueID hinterlegt sind. 
Wenn ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference gleich sind, dann gelten sie als Duplikate und werden entfernt.
*/
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference ORDER BY UniqueID ) AS row_num 
FROM
    nashvillehousing;

    
/*
Ich schreibe denselben Code mithilfe von Subqueries. 104 Duplikate wurden entdeckt.
Bevor Delete verschaffe ich mit Select eine Übersicht und überprüfe nochmal alles.
Nach Delete wird nichts mehr angezeigt.
*/
SELECT 
	* 
FROM 
	nashvillehousing
WHERE 
	UniqueID NOT IN 
	(
		SELECT 
			UniqueID
		FROM 
		(
			SELECT
				UniqueID,
				ROW_NUMBER() OVER (PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference ORDER BY UniqueID) AS row_num
			FROM
				nashvillehousing
		) AS RowNumSubquery
		WHERE 
			row_num = 1
	);
    
-- 104 Duplikate werden entfernt
DELETE
FROM
    nashvillehousing 
WHERE
    UniqueID NOT IN 
    (
        SELECT
            UniqueID 
        FROM
            (
                SELECT
                    UniqueID,
                    ROW_NUMBER() OVER (PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference ORDER BY UniqueID) AS row_num 
                FROM
                    nashvillehousing 
            ) AS RowNumSubquery 
        WHERE
            row_num = 1 
    );
    
-- Nicht mehr benötigte (alte) Spalten entfernen.
ALTER TABLE nashvillehousing
DROP COLUMN PropertyAddress;

ALTER TABLE nashvillehousing
DROP COLUMN OwnerAddress;

ALTER TABLE nashvillehousing
DROP COLUMN TaxDistrict;

