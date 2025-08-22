-- More than 8 NAICS updated
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE TABLE si_dataops_prod.daas.DAX_12301_prep_ML AS                                                       
SELECT DISTINCT
	 company_id
	,CASE
		WHEN LENGTH(naics) >= 2 THEN SUBSTR(naics, 0, 2)
		ELSE naics
	 END AS naics_code
FROM
	 si_dataops_prod.daas.o_cds_naics cns
WHERE NULLIF(naics, '\N') IS NOT NULL
ORDER BY 1 
--LIMIT 1000
; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
 
DROP TABLE si_dataops_prod.daas.DAX_12301_prep2_ML; 

--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE si_dataops_prod.daas.DAX_12301_prep2_ML AS
SELECT *, COUNT(naics_code) OVER(PARTITION BY company_id) AS dupe 
FROM si_dataops_prod.daas.DAX_12301_prep_ML 
--WHERE company_id = 70051516 
;   

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 

DROP TABLE daas.DAX_12301_prep3_ML ;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

CREATE TABLE daas.DAX_12301_prep3_ML AS 
SELECT DISTINCT company_id FROM si_dataops_prod.daas.DAX_12301_prep2_ML WHERE dupe >= 8 
;


--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
 
DROP TABLE si_dataops_prod.daas.DAX_12301_prep4_ML ;   

-- 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
--

CREATE TABLE si_dataops_prod.daas.DAX_12301_prep4_ML  AS
SELECT DISTINCT 
	 company_id 
	,ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(naics)), '|') AS naics_code
FROM 
	 si_dataops_prod.daas.o_cds_naics
WHERE
	 NULLIF(naics, '\N') IS NOT NULL
GROUP BY company_id
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

DROP TABLE si_dataops_prod.daas.DAX_12301_results_ML; 

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

--CREATE TABLE si_dataops_prod.daas.DAX_12301_results_ML AS 
SELECT DISTINCT
	 p.company_id
	,oc.primary_domain
	,oc.primary_name
	,oc.street1
	,oc.city
	,oc.state
	,oc.postal_code
	,oc.company_country
	,oc.digits
	,p4.naics_code
	,oc.linkedin_url 
	,oc.employee_count
	,oc.revenue
	,oc.company_status
FROM si_dataops_prod.daas.DAX_12301_prep3_ML p
LEFT JOIN si_dataops_prod.daas.o_cds oc ON p.company_id = oc.company_id 
LEFT JOIN si_dataops_prod.daas.DAX_12301_prep4_ML p4 ON p.company_id = p4.company_id
WHERE oc.company_status IN ('whitelist', 'active') AND oc.is_hq IS TRUE-- AND oc.company_country = UPPER(oc.company_country) 
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--

SELECT * from daas.DAX_12301_results_ML ;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data need to be delivered with this Query
  
SELECT Sector , ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(naics)), ';') FROM
( 
	SELECT DISTINCT LEFT(naics,2) as naics , Sector  FROM daas.master_naics_code_desc_05242024 
	GROUP BY ALL HAVING COUNT(DISTINCT Sector) = 1
) AS SUBQUERRY GROUP BY ALL
;

--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--






