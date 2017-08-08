# Accounting for Openstack

Accounting for Openstack, uses hdf5 to store time series of number of VCPUs, amount of memmory, amount of local disk, aamount of cinder volumes

## Configuration

The scripts rely on environment variables:

* OUT_DIR - output directory for projects.json, also for the accounting files the default value is '/tmp' 
* MYSQL_USER - database user to get the records from the cinder DB
* MYSQL_PASS - database password for the cinder DB
* MYSQL_HOST - database host
* All Openstack admin credentials - OS_ variables

## Usage

The script get_os_projects.sh creates a json with all projects to be accounted for

```
./get_os_projects.sh
```


