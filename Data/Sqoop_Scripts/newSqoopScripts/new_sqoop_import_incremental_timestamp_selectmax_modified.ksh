#!/bin/ksh

#
# Added  on 27 JULY 2016
#--fields-terminated-by '\001' --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --relaxed-isolation --split-by ${SPLIT} --delete-target-dir --fetch-size 75000 --target-dir ${RUTA} -m ${HILOS} \
#
	

#exit 0

# LISTA DE PARAMETROS UTILIZADOS
# ==============================
# $1 = NUMERO DE HILOS ABIERTOS DESDE SQOOP - Number of threads
# $2 = NOMBRE DEL GRUPO FUNCIONAL - Functional group
# $3 = CAMPO DEL INCREMENTAL - Timestamp Column on source table
# $4 = FECHA DE CARGA (data_date_part) (aaaa-mm-dd) - Loading Date
# $5 = NOMBRE DE LA TABLA - Table Name
# $6 = FECHA DESDE  - Date from
# $7 = FECHA HASTA - Date to
# $8 = MAPPERS DE LA TABLA (split) - Number of mappers
# $9 = passid
# $10 = entity


HORAINI=`date "+%F %T"`
TIMESTAMP_PART=`date "+%s"`
# updating default temp location
export _JAVA_OPTIONS=-Djava.io.tmpdir=/data/1/ingestion_process_temp/

#kinit ingstpro -k -t /home/ingstpro/krb5.ingst.keytab
#kinit ingstpre -k -t /controlm/ingstpre/ingstpre.keytab
kinit ingstpre -k -t /home/DOMAIN_USERS/PRBUK/BigDataTeam/KEYTABS/ingstpre.keytab

if ! /santanderukretail/Metamodelo/componente/conf/create_component_setting_config.sh
then
  echo "Settings for Encryption has not been properly set. Exiting!"
  exit 1
fi

CONN_STR=([DAEG]='jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1' [DAUG]='jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1' [DHEG]='jdbc
:db2://choq.cahoot.pre.corp:5002/DHTG' [DUPG]='jdbc:db2://comq.cbuk.pre.corp:5002/DUQG' )
DB=`grep -i ${2} /santanderukretail/ControlM/mainframe/scripts/tables.txt |grep -i ${5},| awk -F, '{print $3}'`
#FUN_GRP=`grep -i ${5} tables.txt | awk -F, '{print $3}'`


SCHEMA_CON=([DAEG]='ORG01' [DAUG]='ORG01' [DHEG]='ORG0H' [DUPG]='ORG01' )
SCHEMA=${SCHEMA_CON[$DB]}

DBCONNSTR=${CONN_STR[$DB]}

echo "#################"
echo $DBCONNSTR
echo $SCHEMA
echo " Time Stamp Part : ${TIMESTAMP_PART}"
echo "##################"



HILOS=$1
DIR=$2
CAMPO=$3
FECHA=$4
TABLA=$5
TABLA_DIR=`echo ${TABLA} | tr [:upper:] [:lower:]`
#FECHA_INC_INI=$6
#FECHA_INC_FIN=$7
SPLIT=$6
CURRENT_TIMESTAMP=$7
passid=$8
entity=$9
echo "#################"
echo " Current timestamp from controlM : ${CURRENT_TIMESTAMP}"
echo "##################"

TIPO="incremental"
ORIGEN="database_db2"
#PREFIJO="bu"
#PREFIJO="bu_retail"
PREFIJO="bu_${entity}"
BBDD_HIVE=${DIR}
TABLA_HIVE=${TABLA_DIR}
DB_HIST=${PREFIJO}_${DIR}_hist
echo $DB_HIST
#RUTA_COPY="/prod/landing/modelado/db2/part-m-00000"
RUTA_COPY="/retailuk/landing/modelado/db2/${DB}/part-m-00000"
LIBJAR="/var/lib/sqoop/db2jcc_license_cisuz.jar"

## Componemos el valor del data_date_part
ANO=${FECHA:0:4}
MES=${FECHA:4:2}
DIA=${FECHA:6:2}

# UK Variables
LIBJAR="/var/lib/sqoop/db2jcc_license_cisuz.jar"
#QUEUE_NAME=root.default
QUEUE_NAME=root.ingest
CRYPT_PHRASE=sqoop2
#CRYPT_FILE=/prod/landing/config/DAEG.enc
#CRYPT_FILE=/retailuk/landing/config/DAEG.enc
#CRYPT_FILE=/retailuk/landing/config/pcm_retail.enc
ENC_PATH=`grep -i ${passid} /santanderuk${entity}/ControlM/mainframe/scripts/config.txt |grep -i ${entity}, |cut -d',' -f3`
CRYPT_FILE=$ENC_PATH

#DB Settings
#DBUSERN=BDTSQOOP
#DBUSERN=PCMSQOOP
DBUSERN=$8
#DBCONNSTR="jdbc:db2://ukabbeydaeg.perplex.ibm.anplc.co.uk:5026/UKABBEYDAEG"
#DBCONNSTR="jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1"

#RUTA="/prod/landing/${DIR}/${TABLA_DIR}/data_date_part=${ANO}-${MES}-${DIA}/data_timestamp_part=${TIMESTAMP_PART}"

#Querying respective hive tables and getting max key value, passing that key value to Sqoop where condition 
KEY=$CAMPO

HDFS_Location=`hdfs dfs -ls /retailuk/landing/${DIR}/${TABLA_DIR}`

if [ -n "$HDFS_Location" ]; then
count=0
REPEAT=true
while $REPEAT; do

#maxValueOutput=$(hive -e  "select max($KEY)as Max_value from $DB_HIST.$TABLA_HIVE; ")
#maxValueOutput=$( beeline -u "jdbc:hive2://hiveanadc1.analytics1.santanderuk.pre.corp:10001/default;principal=hive/hiveanadc1.analytics1.santanderuk.pre.corp@SANTANDERUK.CORP" -n  hive -p hive -e "select max($KEY)as Max_value from $DB_HIST.$TABLA_HIVE; ")
#tempMaxValue=$(echo -e "$maxValueOutput" | sed -n '4p')
#echo $tempMaxValue
maxValue=0
#maxValue=$(echo $tempMaxValue  |sed 's/^..//g' |sed 's/..$//g'| sed 's/ /-/g'|sed 's/:/./g')
maxValue="2017-02-14-08.11.14.649644"
let count++
echo $maxValue
echo "-----------"
if [  "$maxValue" -eq "NULL" ]; then
maxValue=""
echo $maxValue
REPEAT=false
fi
echo $maxValue
if [ "$maxValue" = "" ] && [ "$REPEAT" = true ]; then
echo "Query Failed about to sleep"
sleep 1
else
REPEAT=false
echo "setting to false"
echo $REPEAT

fi
if [[ "$count" -gt 3 ]]; then
       echo "Hive Error : Query Max $count execution time reached; Exiting Sqoop!"
       exit 1;
fi

done

else
varWhere=""
fi

echo "'$maxValue'"
varWhere=""

if [ -n "$maxValue" ]; then
        varWhere="--where \"${CAMPO} > '${maxValue}' AND ${CAMPO} < '${CURRENT_TIMESTAMP}'\""
else
    varWhere="--where \"${CAMPO} < '${CURRENT_TIMESTAMP}'\"" 
fi

echo $varWhere
#exit 0 
RUTA="/retailuk/landing/${DIR}/${TABLA_DIR}/data_date_part=${ANO}-${MES}-${DIA}/data_timestamp_part=${TIMESTAMP_PART}"
#sleep 1m
echo $varWhere
sqoop import -Dmapred.job.queue.name=${QUEUE_NAME} -libjars ${LIBJAR} \
-Dorg.apache.sqoop.credentials.loader.class=org.apache.sqoop.util.password.CryptoFileLoader -Dorg.apache.sqoop.credentials.loader.crypto.passphrase=${CRYPT_PHRASE} \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--table ${SCHEMA}.${TABLA} -driver com.ibm.db2.jcc.DB2Driver --connect "${DBCONNSTR}" \
--username $DBUSERN --password-file $CRYPT_FILE \
--fields-terminated-by '\001' --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --relaxed-isolation --fetch-size 75000 --split-by ${SPLIT} --delete-target-dir \
#--target-dir ${RUTA} "$varWhere" -m ${HILOS} 
--target-dir ${RUTA} `echo $varWhere` -m ${HILOS} 
code=$?

echo; echo "LLAMADA INVENTARIO"
. /santanderukretail/Metamodelo/llamada_inventario.ksh

if [ ${code} != 0 ]
	then
		echo "Error al realizar el sqoop import de la tabla ${TABLA}"
        exit ${code}
fi

echo;echo "LLAMADA TABLAS"
. /santanderukretail/Metamodelo/llamada_tablas.ksh

code=$?
if [ ${code} != 0 ]
        then
                echo "Error in llamada_tablas.ksh query"
                exit ${code}
fi

exit 0
