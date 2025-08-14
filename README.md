# thlb-proxy
FAIB_PROXY_THLB git repo is a collection of R scripts developed by the BC Government’s Forest Analysis and Inventory Branch (FAIB) to generate a provincial-scale raster based proxy Timber Harvesting Land Base (THLB) at a 100m x 100m resolution. The workflow imports multiple spatial and tabular datasets from shared Prov Gov drives and the BCGW into a PostgreSQL/PostGIS database. A series of business rules, data driven filters, and assumptions are applied to approximate four nested land bases: Forest Management Land Base (FMLB), Forest Assessment Land Base (FALB), Proxy Analysis Forested Land Base (pAFLB) and Proxy Timber Harvesting Land Base (pTHLB).

## How to Run

### Installation

```
git clone https://github.com/bcgov/FAIB_PROXY_THLB.git
cd FAIB_PROXY_THLB
```

### 1. Install supporting software on PC

#### Postgres
 - Requires PostgreSQL database (version 12 or above). During installation, be sure to install the dependencies for `postgis` and `postgis_raster` and `oracle_fdw`. 

1. Create a database, preferably: `thlb_proxy`
2. Once PostgreSQL and dependencies are installed, enable database with the following extensions enabled:
 ```
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS oracle_fdw;
 ```

3. Create two recommended schemas:
 ```
CREATE SCHEMA IF NOT EXISTS raster;
CREATE SCHEMA IF NOT EXISTS whse;
 ```

#### Install Packages in R
 - Installed version of R Version 4.0 or above (https://cran.r-project.org/bin/windows/base/)

 - Requires the following R packages:
 ```
 install.packages("RPostgres")
 install.packages("glue")
 install.packages("terra")
 install.packages("keyring")
 install.packages("sf")
 install.packages("devtools")
 library(devtools)
 install_github("bcgov/FAIB_DADMTOOLS")
 ```

1. Set up Database Connections in R 
The dadmtools library uses the Windows Credential Manager Keyring to manage passwords. Two keyrings are recommended for most usage within the library when connecting to local postgres databases and/or oracle databases. Instructions below show how to create two required keyrings: `localsql` and `oracle`.  Note to connect to a database without keyring, you can pass a list to dadmtools functioons containing the following arguments: list(driver = driver, host = host, user = user, dbname = dbname, password = password, port = port).

Set up "localpsql" keyring:
```
library(keyring)
keyring_create("localpsql")
key_set("dbuser", keyring = "localpsql", prompt = 'Postgres keyring dbuser:')
key_set("dbpass", keyring = "localpsql", prompt = 'Postgres keyring password:')
key_set("dbhost", keyring = "localpsql", prompt = 'Postgres keyring host:')
key_set("dbname", keyring = "localpsql", prompt = 'Postgres keyring dbname:') ## thlb_proxy
```

Set up "oracle" keyring:
```
keyring_create("oracle")
key_set("dbuser", keyring = "oracle", prompt = 'Oracle keyring dbuser:')
key_set("dbpass", keyring = "oracle", prompt = 'Oracle keyring password:')
key_set("dbhost", keyring = "oracle", prompt = 'Oracle keyring host:')
key_set("dbservicename", keyring = "oracle", prompt = 'Oracle keyring serviceName:')
key_set("dbserver", keyring = "oracle", prompt = 'Oracle keyring server:')
```