-- Count the number of atheletes from each country

SELECT 
 Country,
 COUNT(PersonName) as TotalAtheletes
FROM athletes
GROUP BY Country

-- Calculate the total medals won by each country

SELECT 
 TeamCountry,
 SUM(Gold) "TotalGold",
 SUM(Silver) "TotalSilver",
 SUM(Bronze) "TotalBronze"
FROM medals
GROUP BY TeamCountry
ORDER BY TotalGold DESC;

-- Find the discipline with the 5 highest participation of female athletes.

SELECT TOP 5
 Discipline,
 Female
FROM entriesgender
ORDER BY Female DESC;

-- Show all coaches from Argentina who coach in 'Volleyball'.

SELECT 
 Name, 
 Country,
 Discipline
FROM coaches
WHERE Country = 'Argentina' AND Discipline = 'Volleyball'
ORDER BY Name;

