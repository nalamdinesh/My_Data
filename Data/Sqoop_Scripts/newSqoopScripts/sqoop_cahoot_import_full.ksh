#!/bin/ksh

# LISTA DE PARAMETROS UTILIZADOS
# ==============================
# $1 = NUMERO DE HILOS ABIERTOS DESDE SQOOP
# $2 = NOMBRE DEL GRUPO FUNCIONAL
# $3 = FECHA DE CARGA (data_date_part) (aaaa-mm-dd)
# $4 = NOMBRE DE LA TABLA
# $5 = MAPPERS DE LA TABLA (split)
# $6 = CAMPO DE PARTICIONADO PARA HIVE EN EL CASO DE PRIMERA CARGA FULL DE TABLAS INCREMENTALES. TENDRA VALOR NULL PARA EL RESTO DE INGESTAS

HORAINI=`date "+%F %T"`
TIMESTAMP_PART=`date "+%s"`

#kinit ingstpro -k -t /home/ingstpro/krb5.ingst.keytab
kinit ingstpre -k -t /controlm/ingstpre/ingstpre.keytab

if ! /santanderukretail/Metamodelo/componente/conf/create_component_setting_config.sh
then
  echo "Settings for Encryption has not been properly set. Exiting!"
  exit 1
fi


HILOS=$1  # JM: Los parametros los pasa ControlM
DIR=$2
FECHA=$3
TABLA=$4
SPLIT=$5
TABLA_DIR=`echo ${TABLA} | tr [:upper:] [:lower:]`
TIPO="full"
CAMPO=$6
ORIGEN="database_db2"
#PREFIJO="bu"
PREFIJO="bu_retail"
BBDD_HIVE=${DIR}
TABLA_HIVE=${TABLA_DIR}
#RUTA_COPY="/prod/landing/modelado/db2/part-m-00000"
#RUTA_COPY="/retailuk/landing/modelado/db2/part-m-00000"
RUTA_COPY="/retailuk/landing/modelado/db2/DHTG/part-m-00000"
#SCHEMA="EXA01"
SCHEMA="org01"
SCHEMA="ORG0H"

# UK Variables
LIBJAR="/var/lib/sqoop/db2jcc_license_cisuz.jar"
QUEUE_NAME=root.default
CRYPT_PHRASE=sqoop2
#CRYPT_FILE=/prod/landing/config/DAEG.enc
CRYPT_FILE=/retailuk/landing/config/DAEG.enc

#DB Settings
DBUSERN=BDTSQOOP
#DBCONNSTR="jdbc:db2://ukabbeydbt1.perplex.ibm.anplc.co.uk:5027/UKABBEYDBT1"
DBCONNSTR="jdbc:db2://choq.cahoot.pre.corp:5002/DHTG"

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
--fields-terminated-by '\001' --relaxed-isolation --split-by ${SPLIT} --delete-target-dir --fetch-size 75000 --target-dir ${RUTA} -m ${HILOS}


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
