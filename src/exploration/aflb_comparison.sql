WITH a AS (
select 
	proxy.man_unit,
	max(tsr_report_year) AS tsr_report_year,
	TO_CHAR(sum(thlb.aflb_fact),'FM999,999,999,999')  as thlb_aflb,	TO_CHAR(sum(proxy.aflb),'FM999,999,999,999') as proxy_aflb,	TO_CHAR(round((sum(thlb.aflb_fact) - sum(proxy.aflb))),'FM999,999,999,999') AS diff,	TO_CHAR((sum(thlb.aflb_fact) - sum(proxy.aflb))/sum(proxy.aflb)*100,'FM999,999,999,999.0')
From
thlb_proxy.prov_netdown proxy
LEFT join whse.prov_thlb_tsas_gr_skey thlb_key USING (gr_skey)
LEFT join whse.prov_thlb_tsas thlb USING (pgid)
WHERE man_unit ilike '%TSA'
group by 
	proxy.man_unit
)
SELECT
 * FROM a
WHERE tsr_report_year > 2015
ORDER BY tsr_report_year desc




-- loreen tsa 27 revelstoke
SELECT
-- sum(proxy.aflb - tsa.aflb),
sum(proxy.aflb - tsa.aflb),
n01_fmlb, 
n02_ownership,
n03_ownership
FROM
public.tsa27_netdown20241002_table_final tsa
JOIN
thlb_proxy.prov_netdown proxy USING (gr_skey)
WHERE man_unit = 'Revelstoke TSA'
AND 
tsa.aflb > proxy.aflb
-- proxy.aflb > tsa.aflb
group BY
n01_fmlb, 
n02_ownership,
n03_ownership;

-- Proxy > TSA (Ie. Rhian removed)
-- Out of 27,622 ha
SELECT
sum(proxy.aflb - tsa.cflb_fact),
sum(proxy.aflb),
sum(tsa.cflb_fact),
tsa.n01_nonprov,
tsa.n02_nontsa,
tsa.n03_nonfrst,
sum(tsa.p04_road - proxy.p05_linear_features)
FROM
public.tsa13_netdown tsa
JOIN
thlb_proxy.prov_netdown proxy USING (gr_skey)
WHERE man_unit = 'Kootenay Lake TSA'
AND 
-- tsa.cflb_fact > proxy.aflb
proxy.aflb > tsa.cflb_fact
group by 
n01_nonprov,
n02_nontsa,
n03_nonfrst;

-- kootenay lakes
SELECT
sum(tsa.cflb_fact) as tsa_thlb,
sum(proxy.aflb) as proxy_thlb,
n01_nonprov,
n02_nontsa,
n03_nonfrst,
p04_road
FROM
public.tsa13_netdown tsa
JOIN
thlb_proxy.prov_netdown proxy USING (gr_skey)
WHERE man_unit = 'Kootenay Lake TSA'
AND 
-- tsa.aflb > proxy.aflb
proxy.aflb > tsa.aflb
group by 
n01_nonprov,
n02_nontsa,
n03_nonfrst,
p04_road

-- robson valley
SELECT
sum(proxy.aflb - tsa.aflb),
n01_fmlb, 
n02_ownership,
n03_ownership

FROM
public.tsa17_netdown2024 tsa
JOIN
thlb_proxy.prov_netdown proxy USING (gr_skey)
WHERE man_unit = 'Robson Valley TSA'
AND 
tsa.aflb > proxy.aflb
-- proxy.aflb > tsa.aflb
group BY
n01_fmlb, 
n02_ownership,
n03_ownership;

-- boundary TSA 
SELECT
-- sum(proxy.aflb - tsa.aflb),
-- n01_fmlb, 
-- n02_ownership,
-- n03_ownership
sum(proxy.aflb),
sum(tsa.aflb::double precision)

FROM
public.tsa02_netdown2024 tsa
JOIN
thlb_proxy.prov_netdown proxy USING (gr_skey)
WHERE man_unit = 'Boundary TSA';

-- nass


select 
proxy.n08_rec,
proxy.n09_misc,
round(sum(tsa.thlb_net::numeric), 0) as tsa_thlb,
round(sum(proxy.thlb_net)::numeric, 0) as proxy_thlb,
round(sum(tsa.cflb_fact::numeric), 0) as tsa_aflb,
round(sum(proxy.aflb)::numeric, 0) as proxy_aflb
from 
public.tsa43_netdown tsa
JOIN
thlb_proxy.prov_netdown proxy USING (gr_skey)
WHERE man_unit = 'Nass TSA'
GROUP BY 
proxy.n08_rec,
proxy.n09_misc