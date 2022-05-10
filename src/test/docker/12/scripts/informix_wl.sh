#!/bin/bash
#
#  name:        informix_wl.sh:
#  description: Starts WL in Docker container
#

OPT=$1

# PORT_SQLEXEC=0x0001
#    PORT_DRDA=0x0002
#   PORT_MONGO=0x0010
#    PORT_REST=0x0020
#    PORT_MQTT=0x0040

main()
{
###
###  Setup environment
###
. /usr/local/bin/informix_inf.env

dt=`date`
MSGLOG ">>>    Starting WL ($dt) ... $OPT" N

if [[ $(($OPT & $PORT_MONGO)) == $(($PORT_MONGO)) ]]
then
   setupMongoProp
fi
if [[ $(($OPT & $PORT_REST)) == $(($PORT_REST)) ]]
then
   setupRestProp
fi
if [[ $(($OPT & $PORT_MQTT)) == $(($PORT_MQTT)) ]]
then
   setupMqttProp
fi

startWL $OPT


if [[ $(($OPT & $PORT_MONGO)) == $(($PORT_MONGO)) ]]
then
   checkMongoRunning
fi
if [[ $(($OPT & $PORT_REST)) == $(($PORT_REST)) ]]
then
   checkRestRunning
fi
if [[ $(($OPT & $PORT_MQTT)) == $(($PORT_MQTT)) ]]
then
   checkMqttRunning
fi



MSGLOG ">>>    [COMPLETED]" N

}


#####################################################################
### FUNCTION DEFINITIONS
#####################################################################

SUCCESS=0
FAILURE=-1

TCP_PORT=9088
MONGO_PORT=27017
REST_PORT=27018
MQTT_PORT=27883

MONGO_PROP=$INFORMIXDIR/etc/json_mongo.properties
CUSTOM_MONGO_PROP=$INFORMIXDIR/etc/custom_mongo.properties

REST_PROP=$INFORMIXDIR/etc/json_rest.properties
CUSTOM_REST_PROP=$INFORMIXDIR/etc/custom_rest.properties

MQTT_PROP=$INFORMIXDIR/etc/json_mqtt.properties
CUSTOM_MQTT_PROP=$INFORMIXDIR/etc/custom_mqtt.properties

WL_LOG=$INFORMIXDIR/etc/json_listener_logging.log

###
### isMongoListening - Determine if mongo WL port is in use 
###
function isMongoListening()
{
sudo netstat -npl | grep -E "^.*:${MONGO_PORT}.+LISTEN.*"
mongoListening=$?

if [[ "${mongoListening}" -eq 0 ]]
then
   return $FAILURE
else
   return $SUCCESS
fi

}

###
### checkMongoRunning- Determine if mongo WL is running 
###
function checkMongoRunning()
{

MONGOANS=$(ps -aef | grep -c $CUSTOM_MONGO_PROP) 
if [[ "${MONGOANS}" -eq 2 ]];
then
	sudo netstat -npl | grep ${MONGO_PORT}
	if [ $? -eq 0 ]
	then
		echo " Wire Listener Mongo Port : "${MONGO_PORT}
	fi
fi

}

###
### isRestListening - Determine if rest WL port is in use 
###
function isRestListening()
{
sudo netstat -npl | grep -E "^.*:${REST_PORT}.+LISTEN.*"
restListening=$?

if [[ "${restListening}" -eq 0 ]]
then
   return $FAILURE
else
   return $SUCCESS
fi

}

###
### checkRestRunning - Determine if rest WL is running 
###
function checkRestRunning()
{
RESTANS=$(ps -aef | grep -c $CUSTOM_REST_PROP)
if [ "${RESTANS}" -eq 2 ]
then
	sudo netstat -npl | grep ${REST_PORT}
	if [ $? -eq 0 ]
	then
		echo " Wire Listener Rest Port : "${REST_PORT}
	fi
fi	

}

###
### isMqttListening - Determine if mqtt port is in use 
###
function isMqttListening()
{
sudo netstat -npl | grep -E "^.*:${MQTT_PORT}.+LISTEN.*"
mqttListening=$?

if [[ "${mqttListening}" -eq 0 ]]
then
   return $FAILURE
else
   return $SUCCESS
fi

}

###
### checkMqttRunning - Determine if mqtt WL is running 
###
function checkMqttRunning()
{
MQTTANS=$(ps -aef | grep -c $CUSTOM_MQTT_PROP)
if [ "${MQTTANS}" -eq 2 ]
then
	sudo netstat -npl | grep ${MQTT_PORT}
	if [ $? -eq 0 ]
	then
		echo " Wire Listener MQTT Port : "${MQTT_PORT}
	fi
fi	

}


###
###
### startWL - Start the WL 
###
function startWL()
{
if (! isMongoListening) || (! isRestListening) || (! isMqttListening)
then
	MSGLOG "${MONGO_PORT}, ${REST_PORT}, ${MQTT_PORT} Port is bound to some other service" N
else

	# Starting listener types
   cmd="java -jar '${INFORMIXDIR}'/bin/jsonListener.jar  "
   if [[ $(($OPT & $PORT_REST)) == $(($PORT_REST)) ]]
   then
      cmd+=" -config $REST_PROP " 
   fi
   if [[ $(($OPT & $PORT_MONGO)) == $(($PORT_MONGO)) ]]
   then
      cmd+=" -config $MONGO_PROP " 
   fi
   if [[ $(($OPT & $PORT_MQTT)) == $(($PORT_MQTT)) ]]
   then
      cmd+=" -config $MQTT_PROP" 
   fi
   cmd+=" -logfile $WL_LOG" 
   cmd+=" -loglevel info -start &" 

	# java -jar "${INFORMIXDIR}"/bin/jsonListener.jar  \
	# 	-config $REST_PROP \
	# 	-config $MONGO_PROP \
	# 	-config $MQTT_PROP \
	# 	-logFile $WL_LOG \
	# 	-loglevel info \
	# 	-start &
   MSGLOG ">>>    WL CMD: $cmd " N

   eval $cmd 
fi

}


### setupMongoProp- Setup mongo properties file 
###
function setupMongoProp()
{

if [[ ! -f $CUSTOM_MONGO_PROP ]]; then
	touch $CUSTOM_MONGO_PROP
	echo "#For more configuration options, check the jsonListener-example.properties in /opt/IBM/informix/etc" >> $CUSTOM_MONGO_PROP
	echo "#And add them to $CUSTOM_MONGO_PROP" >> $CUSTOM_MONGO_PROP 
fi

cat $CUSTOM_MONGO_PROP > $MONGO_PROP
echo "listener.type=mongo" >> $MONGO_PROP 
echo "listener.port=$MONGO_PORT" >> $MONGO_PROP 
echo "listener.hostName=$HOSTNAME" >> $MONGO_PROP 
echo "url=jdbc:informix-sqli://${HOSTNAME}:$TCP_PORT/${DB_NAME:=sysmaster}:INFORMIXSERVER=${INFORMIXSERVER};USER=${DB_USER:=informix};PASSWORD=${DB_PASS:=in4mix}" >> $MONGO_PROP 

}


###
### setupRestProp- setup rest properties file 
###
function setupRestProp()
{

if [[ ! -f $CUSTOM_REST_PROP ]]; then
	touch $CUSTOM_REST_PROP
	echo "#For more configuration options, check the jsonListener-example.properties in /opt/IBM/informix/etc" >> $CUSTOM_REST_PROP
	echo "#And add them to $CUSTOM_REST_PROP" >> $CUSTOM_REST_PROP
fi
cat $CUSTOM_REST_PROP > $REST_PROP
echo "listener.type=rest" >> $REST_PROP
echo "listener.port=$REST_PORT" >> $REST_PROP
echo "listener.hostName=$HOSTNAME" >> $REST_PROP
echo "url=jdbc:informix-sqli://${HOSTNAME}:$TCP_PORT/${DB_NAME:=sysmaster}:INFORMIXSERVER=${INFORMIXSERVER};USER=${DB_USER:=informix};PASSWORD=${DB_PASS:=in4mix}"  >> $REST_PROP

}



###
### setupMqttProp- setup mqtt properties file 
###
function setupMqttProp()
{

if [[ ! -f $CUSTOM_MQTT_PROP ]]; then
	touch $CUSTOM_MQTT_PROP
	echo "#For more configuration options, check the jsonListener-example.properties in /opt/IBM/informix/etc" >> $CUSTOM_MQTT_PROP
	echo "#And add them to $CUSTOM_MQTT_PROP" >> $CUSTOM_MQTT_PROP
fi
cat $CUSTOM_MQTT_PROP > $MQTT_PROP
echo "listener.type=mqtt" >> $MQTT_PROP
echo "listener.port=$MQTT_PORT" >> $MQTT_PROP
echo "listener.hostName=$HOSTNAME" >> $MQTT_PROP
echo "url=jdbc:informix-sqli://${HOSTNAME}:$TCP_PORT/${DB_NAME:=sysmaster}:INFORMIXSERVER=${INFORMIXSERVER};USER=${DB_USER:=informix};PASSWORD=${DB_PASS:=in4mix}"  >> $MQTT_PROP
}

###
### MSGLOG
###
function MSGLOG()
{
if [[ $2 = "N" ]]
then
   printf "%s\n" "$1" |tee -a $INIT_LOG
else
   printf "%s" "$1" |tee -a $INIT_LOG
fi
}


###
###  Call to main
###
main "$@"
