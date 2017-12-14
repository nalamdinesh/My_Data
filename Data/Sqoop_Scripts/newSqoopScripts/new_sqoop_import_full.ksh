#!/bin/ksh

# LISTA DE PARAMETROS UTILIZADOS
# ==============================
# $1 = NUMERO DE HILOS ABIERTOS DESDE SQOOP
# $2 = NOMBRE DEL GRUPO FUNCIONAL
# $3 = FECHA DE CARGA (data_date_part) (aaaa-mm-dd)
# $4 = NOMBRE DE LA TABLA
# $5 = MAPPERS DE LA TABLA (split)
# $6 = CAMPO DE PARTICIONADO PARA HIVE EN EL CASO DE PRIMERA CARGA FULL DE TABLAS INCREMENTALES. TENDRA VALOR NULL PARA EL RESTO DE INGESTAS
# $7 = passid
# $8 = entity

HORAINI=`date "+%F %T"`
TIMESTAMP_PART=`date "+%s"`

#kinit ingstpro -k -t /home/ingstpro/krb5.ingst.keytab
kinit ingstpre -k -t /controlm/ingstpre/ingstpre.keytab



if ! /santanderukretail/Metamodelo/componente/conf/create_component_setting_config.sh
then
  echo "Settings for Encryption has not been properly set. Exiting!"
  exit 1
fi
# updating default temp location
export _JAVA_OPTIONS=-Djava.io.tmpdir=/data/1/ingestion_process_temp/

CONN_STR=([DAEG]='jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1' [DAUG]='jdbc:db2://ukabbeydapg.perplex.ibm.anplc.co.uk:5040/UKABBEYDAPG' [DHEG]='jdbc:db2://choq.cahoot.pre.corp:5002/DHTG' [SYSIBM]='jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1' )
DB=`grep -i ^${2} /santanderukretail/ControlM/mainframe/scripts/tables.txt |grep -i ${4},| awk -F, '{print $3}'`
#FUN_GRP=`grep -i ^${4}, tables.txt | awk -F, '{print $3}'`

SCHEMA_CON=([DAEG]='ORG01' [DAUG]='ORG01' [DHEG]='ORG0H' [SYSIBM]='SYSIBM' )
SCHEMA=${SCHEMA_CON[$DB]}

DBCONNSTR=${CONN_STR[$DB]}

echo #################
echo $DBCONNSTR
echo $SCHEMA
echo ##################



HILOS=$1  # JM: Los parametros los pasa ControlM
DIR=$2
FECHA=$3
TABLA=$4
SPLIT=$5
TABLA_DIR=`echo ${TABLA} | tr [:upper:] [:lower:]`
TIPO="full"
CAMPO=$6
passid=$7
entity=$8

ORIGEN="database_db2"
#PREFIJO="bu"
PREFIJO="bu_${entity}"
#PREFIJO="bu_retail"
BBDD_HIVE=${DIR}
TABLA_HIVE=${TABLA_DIR}
#RUTA_COPY="/retailuk/landing/modelado/db2/part-m-00000"
RUTA_COPY="/retailuk/landing/modelado/db2/${DB}/part-m-00000"

#SCHEMA="EXA01"
#SCHEMA="org01"

# UK Variables
LIBJAR="/var/lib/sqoop/db2jcc_license_cisuz.jar"
#QUEUE_NAME=root.default
QUEUE_NAME=root.ingest
CRYPT_PHRASE=sqoop2
#CRYPT_FILE=/prod/landing/config/DAEG.enc
#CRYPT_FILE=/retailuk/landing/config/DAEG.enc

#DB Settings
#DBUSERN=BDTSQOOP
DBUSERN=$7
ENC_PATH=`grep -i ${passid} /santanderuk${entity}/ControlM/mainframe/scripts/config.txt |grep -i ${entity}, |cut -d',' -f3`

CRYPT_FILE=$ENC_PATH

echo $CRYPT_FILE
#exit 0
#DBCONNSTR="jdbc:db2://ukabbeydaeg.perplex.ibm.anplc.co.uk:5026/UKABBEYDAEG"
#DBCONNSTR="jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1"

## Componemos el valor del data_date_part
ANO=${FECHA:0:4}
MES=${FECHA:4:2}
DIA=${FECHA:6:2}

#RUTA="/prod/landing/${DIR}/${TABLA_DIR}/data_date_part=${ANO}-${MES}-${DIA}/data_timestamp_part=${TIMESTAMP_PART}"
RUTA="/retailuk/landing/${DIR}/${TABLA_DIR}/data_date_part=${ANO}-${MES}-${DIA}/data_timestamp_part=${TIMESTAMP_PART}"

sqoop import -Dmapred.job.queue.name=${QUEUE_NAME} -libjars ${LIBJAR} -Dorg.apache.sqoop.credentials.loader.class=org.apache.sqoop.util.password.CryptoFileLoader -Dorg.apache.sqoop.credentials.loader.crypto.passphrase=${CRYPT_PHRASE} \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--table ${SCHEMA}.${TABLA} -driver com.ibm.db2.jcc.DB2Driver --connect "${DBCONNSTR}" \
--username $DBUSERN --password-file $CRYPT_FILE \
--fields-terminated-by '\001' --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --relaxed-isolation --split-by ${SPLIT} --delete-target-dir --fetch-size 75000 --target-dir ${RUTA} -m ${HILOS}


code=$?

. /santanderukretail/Metamodelo/llamada_inventario.ksh # Genera un CSV con el control de lo que se ejecuta a diario. Nueva version a lo largo del dia.

if [ ${code} != 0 ]
	then
		echo "Error al realizar el sqoop import de la tabla ${TABLA}"
        exit ${code}
fi

. /santanderukretail/Metamodelo/llamada_tablas.ksh

code=$?
if [ ${code} != 0 ]
        then
                echo "Error in llamda_tablash.ksh while createing tables"
                exit ${code}
fi

exit 0
