#!/bin/bash
#
#  name:        informix_setup_onconfig.sh:
#  description: Setup the onconfig file 
#  Called by:   informix_entry.sh

OPT=$1

# PORT_SQLEXEC=0x0001
#    PORT_DRDA=0x0002
#   PORT_MONGO=0x0010
#    PORT_REST=0x0020
#    PORT_MQTT=0x0040

#ONCONFIG_PATH=$INFORMIXDIR/etc/$ONCONFIG
ONCONFIG_PATH=$INFORMIX_DATA_DIR/links/$ONCONFIG
if [ -e $INFORMIX_DATA_DIR/$ONCONFIG ]
then
   MSGLOG ">>>        Using $ONCONFIG supplied by user" N
   mv $INFORMIX_DATA_DIR/$ONCONFIG $ONCONFIG_PATH
   sudo chown informix:informix $ONCONFIG_PATH
   sudo chmod 660 $ONCONFIG_PATH
else
   MSGLOG ">>>        Using default $ONCONFIG" N
   cp $INFORMIXDIR/etc/onconfig.std $ONCONFIG_PATH
fi

E_ROOTPATH="$INFORMIX_DATA_DIR/spaces/rootdbs.000"
E_CONSOLE="$INFORMIX_DATA_DIR/logs/console.log"
E_MSGPATH="$INFORMIX_DATA_DIR/logs/online.log"
E_DBSERVERNAME="informix"
E_TAPEDEV="/dev/null"
E_LTAPEDEV="/dev/null"
E_LOCKMODE="row"
E_SBSPACE="sbspace"

sed -i "s#^ROOTPATH .*#ROOTPATH $E_ROOTPATH#g"               "${ONCONFIG_PATH}"
sed -i "s#^CONSOLE .*#CONSOLE $E_CONSOLE#g"                  "${ONCONFIG_PATH}"
sed -i "s#^MSGPATH .*#MSGPATH $E_MSGPATH#g"                  "${ONCONFIG_PATH}"
sed -i "s#^DBSERVERNAME.*#DBSERVERNAME $E_DBSERVERNAME#g"    "${ONCONFIG_PATH}"

if [[ $(($OPT & $PORT_DRDA)) == $(($PORT_DRDA)) ]]
then
sed -i "s#^DBSERVERALIASES.*#DBSERVERALIASES ${E_DBSERVERNAME}_dr#g" "${ONCONFIG_PATH}" 
   MSGLOG "****  Setting dbserveraliases" N
fi

sed -i "s#^TAPEDEV .*#TAPEDEV   $E_TAPEDEV#g"                "${ONCONFIG_PATH}"
sed -i "s#^LTAPEDEV .*#LTAPEDEV $E_LTAPEDEV#g"               "${ONCONFIG_PATH}"
sed -i "s#^DEF_TABLE_LOCKMODE page#DEF_TABLE_LOCKMODE $E_LOCKMODE#g" "${ONCONFIG_PATH}"
sed -i "s#^SBSPACENAME.*#SBSPACENAME $E_SBSPACE#g"               "${ONCONFIG_PATH}"


sudo chown informix:informix "${ONCONFIG_PATH}"
sudo chmod 660 "${ONCONFIG_PATH}"

uSIZE=`echo $SIZE|tr /a-z/ /A-Z/`
uTYPE=`echo $TYPE|tr /a-z/ /A-Z/`


if [[ -z ${uTYPE} ]]
then
    if [[ $uSIZE = "SMALL" ]]
    then
    echo ">>>        Setting up Small System"
    . $SCRIPTS/informix_update_onconfig.sh $ONCONFIG_PATH $SCRIPTS/informix_config.small
    fi

    if [[ $uSIZE = "MEDIUM" ]]
    then
    echo ">>>        Setting up Medium System"
    . $SCRIPTS/informix_update_onconfig.sh $ONCONFIG_PATH $SCRIPTS/informix_config.medium
    fi

    if [[ $uSIZE = "LARGE" ]]
    then
    echo ">>>        Setting up Large System"
    . $SCRIPTS/informix_update_onconfig.sh $ONCONFIG_PATH $SCRIPTS/informix_config.large
    fi

    if [[ $uSIZE = "CUSTOM" ]]
    then
    echo ">>>        Setting up Custom System"
    . $SCRIPTS/informix_update_onconfig.sh $ONCONFIG_PATH $INFORMIX_DATA_DIR/informix_config.custom
    fi

    if [[ -z ${uSIZE} ]]
    then
    echo ">>>        Setting up OLTP/Default system"
    . $SCRIPTS/informix_calculate_onconfig.sh $ONCONFIG_PATH oltp 
    fi
else
    if [[ $uTYPE = "DSS" ]]
    then
    echo ">>>        Setting up DSS system"
    . $SCRIPTS/informix_calculate_onconfig.sh $ONCONFIG_PATH dss 
    fi

    if [[ $uTYPE = "OLTP" ]]
    then
    echo ">>>        Setting up OLTP system"
    . $SCRIPTS/informix_calculate_onconfig.sh $ONCONFIG_PATH oltp 
    fi

    if [[ $uTYPE = "HYBRID" ]]
    then
    echo ">>>        Setting up HYBRID system"
    . $SCRIPTS/informix_calculate_onconfig.sh $ONCONFIG_PATH hybrid 
    fi
fi
