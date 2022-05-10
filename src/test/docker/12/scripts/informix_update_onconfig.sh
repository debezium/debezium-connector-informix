#!/bin/bash
#
#  name:        informix_udpate_onconfig.sh:
#  description: Make modifications to onconfig file 
#  Called by:   informix_entry.sh
#  ONCONFIG_PATH = $1 - ONCONFIG FILE 
#  MODFILE = $2 - the modification file to use 

ADD=1
UPDATE=2
DELETE=3
MOD=0
ONCONFIG_PATH=$1
MODFILE=$2

IFS=" "


while IFS= read -r line || [ -n "$line" ]
do
   toks=( $line )
   if [[ $line == "" ]]
   then
      continue
   fi
   if [[ ${line:0:1} == "#" ]]
   then
      continue
   fi

   if [[ ${toks[0]} == "[ADD]" ]]
   then
   MOD=$ADD
   #echo "Setting ADD"
   continue
   fi
   if [[ ${toks[0]} == "[UPDATE]" ]]
   then
   MOD=$UPDATE
   #echo "Setting UPDATE"
   continue
   fi
   if [[ ${toks[0]} == "[DELETE]" ]]
   then
   MOD=$DELETE
   #echo "Setting DELETE"
   continue
   fi

   #echo "   Process line"
   if [[ $MOD == $DELETE ]]
   then
   #echo "   delete line ${toks[0]}"
   sed -i "/^${toks[0]}/d" $ONCONFIG_PATH
   fi

   if [[ $MOD == $ADD ]]
   then
   #echo "   add line $line"
   cnt=`sed -n "/^${line}/p" $ONCONFIG_PATH |wc -l`
      if [[ $cnt == "0" ]]
      then
         echo $line >> $ONCONFIG_PATH
      fi
   fi

   if [[ $MOD == $UPDATE ]]
   then
   #echo "   update line ${toks[0]}"
   sed -i "s/^${toks[0]}.*/${toks[0]} ${toks[1]}/g" $ONCONFIG_PATH
   fi

done < $MODFILE 
