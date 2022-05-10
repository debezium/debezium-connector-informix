#!/bin/bash
#
#  name:        informix_sizing_onconfig.sh:
#  description: Make modifications to onconfig file  based on container 
#               resources
#  Called by:   informix_setup_onconfig.sh
#   Params:
#       onconfig path (absolute path)
#       Amount of Memory % to use
#       Amount of CPU % to use


main()
{
uTYPE=`echo $2|tr /a-z/ /A-Z/`
ONCONFIG_PATH=$1
vMEM_USEPERC=.80

if [[ $uTYPE == "DSS" ]]
then
   vBUF_PERC="20"
   vSHM_PERC="75"
   vNONPDQ_PERC="5"
elif [[ $uTYPE == "HYBRID" ]]
then
   vBUF_PERC="50"
   vSHM_PERC="49"
   vNONPDQ_PERC="1"
### oltp - default
else
   vBUF_PERC="80"
   vSHM_PERC="19"
   vNONPDQ_PERC="1"
fi

SUCCESS=0
FAILURE=-1

CPUFILE="/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
MEMFILE="/sys/fs/cgroup/memory/memory.limit_in_bytes"

vMEM_LIMIT_B=`cat $MEMFILE`
vMEM_LIMIT_MB=`echo "($vMEM_LIMIT_B / 1024) / 1024" | bc`

vCPUS_LIMIT=`cat $CPUFILE`

vMEM_SYS_MB=`free -m |grep Mem|awk '{print $2}'`
vCPUS_SYS=`lscpu |grep "CPU(s):"|grep -v node|awk '{print $2}'`


###
### Set the amount of CPU's available to the docker container
###
if [[ $vCPUS_LIMIT == "-1" ]]
then
   vCPUS=$vCPUS_SYS
else
   vCPUS=`echo "$vCPUS_LIMIT / 100000" | bc`
fi

###
### Set the amount of Memory available to the docker container
###
if (useSystemMemorySize) 
then
   vMEM_MB=$vMEM_SYS_MB
else
   vMEM_MB=$vMEM_LIMIT_MB
fi


#echo "vMEM_MB  = $vMEM_MB"
#echo "vCPUS = $vCPUS"
#echo "vCPUS_SYS= $vCPUS_SYS"
#echo "vCPUS_LIMIT = $vCPUS_LIMIT"

setCPUResources
setMEMResources
setGenericResources

}



function useSystemMemorySize()
{

   #echo "vMEM_LIMIT_MB = $vMEM_LIMIT_MB"
   #echo "vMEM_SYS_MB = $vMEM_SYS_MB"

   if [ `expr $vMEM_LIMIT_MB` -gt `expr $vMEM_SYS_MB` ]
   then
      return $SUCCESS 
   else
      return $FAILURE 
   fi
   
}


function setCPUResources()
{

   if [ `expr $vCPUS` -gt 1 ]
   then
      E_NUMCPU=`expr $vCPUS - 1`
      E_MULTI=1
   else
      E_NUMCPU=$vCPUS
      E_MULTI=0
   fi
   sed -i "s#^VPCLASS cpu.*#VPCLASS cpu,num=$E_NUMCPU,noage#g" "${ONCONFIG_PATH}"
   sed -i "s#^MULTIPROCESSOR.*#MULTIPROCESSOR $E_MULTI#g" "${ONCONFIG_PATH}"




   ### Small System
   ### Relate to informix_config.small
   if [ `expr $E_NUMCPU` -lt 4 ]
   then
      sed -i "s#^LOCKS.*#LOCKS 50000#g" "${ONCONFIG_PATH}"
      sed -i "s#^LOGBUFF.*#LOGBUFF 128#g" "${ONCONFIG_PATH}"
      sed -i "s#^NETTYPE.*#NETTYPE soctcp,1,200,CPU#g" "${ONCONFIG_PATH}"
      sed -i "s#^PHYSBUFF.*#PHYSBUFF 128#g" "${ONCONFIG_PATH}"
      sed -i "s#^VP_MEMORY_CACHE_KB.*#VP_MEMORY_CACHE_KB 0#g" "${ONCONFIG_PATH}"

   ### Medium System
   ### Relate to informix_config.medium
   elif [ `expr $E_NUMCPU` -lt 8 ]
   then
      sed -i "s#^LOCKS.*#LOCKS 100000#g" "${ONCONFIG_PATH}"
      sed -i "s#^LOGBUFF.*#LOGBUFF 256#g" "${ONCONFIG_PATH}"
      sed -i "s#^NETTYPE.*#NETTYPE soctcp,4,200,CPU#g" "${ONCONFIG_PATH}"
      sed -i "s#^PHYSBUFF.*#PHYSBUFF 256#g" "${ONCONFIG_PATH}"
      sed -i "s#^VP_MEMORY_CACHE_KB.*#VP_MEMORY_CACHE_KB 2048#g" "${ONCONFIG_PATH}"

   ### Large System
   ### Relate to informix_config.large
   else
      sed -i "s#^LOCKS.*#LOCKS 250000#g" "${ONCONFIG_PATH}"
      sed -i "s#^LOGBUFF.*#LOGBUFF 512#g" "${ONCONFIG_PATH}"
      sed -i "s#^NETTYPE.*#NETTYPE soctcp,8,200,CPU#g" "${ONCONFIG_PATH}"
      sed -i "s#^PHYSBUFF.*#PHYSBUFF 512#g" "${ONCONFIG_PATH}"
      sed -i "s#^VP_MEMORY_CACHE_KB.*#VP_MEMORY_CACHE_KB 4096#g" "${ONCONFIG_PATH}"

   fi

}


function setMEMResources()
{

   vMEMUSE_BUF=`echo "scale=2; (( $vMEM_MB  * $vMEM_USEPERC) * ($vBUF_PERC / 100))" | bc`
   vMEMUSE_SHM=`echo "scale=2; (( $vMEM_MB  * $vMEM_USEPERC) * ($vSHM_PERC / 100))" | bc`
   vMEMUSE_NONPDQ=`echo "scale=2; (( $vMEM_MB  * $vMEM_USEPERC) * ($vNONPDQ_PERC/ 100))" | bc`



   vBUFFERS=`echo "$vMEMUSE_BUF * 500 "|bc`
   vSHMVIRTSIZE=`echo "$vMEMUSE_SHM * 1000 "|bc`
   vSHMTOTAL=`echo "($vMEM_MB * $vMEM_USEPERC) * 1000" | bc`
   vNONPDQ=`echo "$vMEMUSE_NONPDQ * 1000" |bc`



#   vMEMUSE_BUF=${vMEMUSE_BUF%.*}
#   vMEMUSE_SHM=${vMEMUSE_SHM%.*}



   vNONPDQ=${vNONPDQ%.*}
   vSHMTOTAL=${vSHMTOTAL%.*}
   vSHMVIRTSIZE=${vSHMVIRTSIZE%.*}
   vBUFFERS=${vBUFFERS%.*}


   sed -i "s#^BUFFERPOOL size=2k.*#BUFFERPOOL size=2k,buffers=$vBUFFERS,lrus=8,lru_min_dirty=50,lru_max_dirty=60#g" "${ONCONFIG_PATH}"
   sed -i "s#^SHMTOTAL.*#SHMTOTAL $vSHMTOTAL#g" "${ONCONFIG_PATH}"
   sed -i "s#^SHMVIRTSIZE.*#SHMVIRTSIZE $vSHMVIRTSIZE#g" "${ONCONFIG_PATH}"
   sed -i "s#^DS_NONPDQ_QUERY_MEM.*#DS_NONPDQ_QUERY_MEM $vNONPDQ#g" "${ONCONFIG_PATH}"



   #echo "vMEM_MB = $vMEM_MB"
   #echo "vMEMUSE_BUF    = $vMEMUSE_BUF"
   #echo "vMEMUSE_SHM    = $vMEMUSE_SHM"
   echo ">>>        Setting DS_NONPDQ_QUERY_MEM = $vNONPDQ"
   echo ">>>        Setting BUFFERS = $vBUFFERS"
   echo ">>>        Setting SHMVIRTSIZE = $vSHMVIRTSIZE"
   echo ">>>        Setting SHMTOTAL = $vSHMTOTAL"



}

function setGenericResources()
{
### Sets the following onconfig params:
###
### AUTO_TUNE 1 
### DIRECT_IO 1
### DUMPSHMEM 0
### RESIDENT -1

   sed -i "s#^AUTO_TUNE.*#AUTO_TUNE 1#g" "${ONCONFIG_PATH}"
   sed -i "s#^DIRECT_IO.*#DIRECT_IO 1#g" "${ONCONFIG_PATH}"
   sed -i "s#^DUMPSHMEM.*#DUMPSHMEM 0#g" "${ONCONFIG_PATH}"
   sed -i "s#^RESIDENT.*#RESIDENT -1#g" "${ONCONFIG_PATH}"

}


### xperf2 
###   9,223,372,036,854,771,712 mem value when not set
###   -1 cpu value when not set
###
###   800000 cpu value when set to 8
###   4194304000 mem value when set to 4000m
###
###   free -m   48095
###   



###
### Call to main
###

main "$@"