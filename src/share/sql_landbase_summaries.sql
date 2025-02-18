
WITH harvest_netdown_adjustment AS (
SELECT
	gr_skey,
	-- recreate what the R netdown does by adjusting physically inoperable to be 1 when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
	CASE 
		WHEN harvest_start_year_calendar IS NOT NULL THEN 0
		ELSE p12_phys_inop
	END AS p12_phys_inop,
	-- recreate what the R netdown does by adjusting merchantability to be NULL when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
	CASE 
		WHEN harvest_start_year_calendar IS NOT NULL THEN NULL
		ELSE n13_merchantability
	END AS n13_merchantability,
	-- recreate what the R netdown does by adjusting non commercial to be NULL when a cutblocks exists (harvest start year is pulled from consolidated cutblocks layer)
	CASE 
		WHEN harvest_start_year_calendar IS NOT NULL THEN NULL
		ELSE n14_non_commercial
	END AS n14_non_commercial	
FROM
public.thlb_proxy_netdown_tsa43
)
select 	
	sum(CASE
			WHEN 
				n01_fmlb IS NULL 
				THEN 1 
			ELSE 0 
		END) as fmlb,
	sum(CASE 
			-- categorical netdowns
			WHEN 
				n01_fmlb IS NULL AND 
				n02_ownership IS NULL 
				THEN 1
			ELSE 0 
		END) as falb,
	sum(CASE
			WHEN 
				n01_fmlb IS NULL AND 
				n02_ownership IS NULL AND 
				n03_ownership IS NULL AND 
				n04_nonfor IS NULL 
				THEN 1 * (1-p05_linear_features)
			ELSE 0 
		END) as aflb,
	sum(CASE 
			WHEN 
				-- categorical netdowns
				n01_fmlb IS NULL AND 
				n02_ownership IS NULL AND 
				n03_ownership IS NULL AND 
				n04_nonfor IS NULL AND 
				n06_parks IS NULL AND 
				n07_wha IS NULL AND 
				n08_misc IS NULL AND 
				n10_arch IS NULL AND
				n11_harvest_restrictions IS NULL AND
				adj.n13_merchantability IS NULL AND
				adj.n14_non_commercial IS NULL
				-- proportional netdowns
				THEN 1 * (1-p05_linear_features) * (1-p09_riparian) * (1-adj.p12_phys_inop) * (1-p15_future_retention)
			ELSE 0 
		END) as THLB
from
public.thlb_proxy_netdown_tsa43 net
JOIN harvest_netdown_adjustment adj USING (gr_skey);
