# Accounting for Openstack

Accounting for Openstack, uses hdf5 to store time series of number of VCPUs, amount of memmory, amount of local disk, number and amount of cinder volumes

## Configuration

The scripts rely on environment variables:

* OUT_DIR - output directory for projects.json, also for the accounting files the default value is '/tmp' 
* MYSQL_USER - database user to get the records from the cinder DB
* MYSQL_PASS - database password for the cinder DB
* MYSQL_HOST - database host
* CARBON_SERVER - graphite/carbon server
* CARBON_PORT - graphite/carbon

## Usage

The script setup.sh deploys the scripts in /usr/local/bin and a configuration file to setup the necessary environment
variables osacc-conf.sh into /etc

```
./setup.sh
```


