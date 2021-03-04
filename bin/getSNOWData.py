#!/usr/bin/python

#######################################################
#
# SNOW REST mediator for topology inclusion into ASM
#
# 02/09/21 - Jason Cress (jcress@us.ibm.com)
#
#######################################################

from httplib import IncompleteRead
import time
import datetime
import gc
import random
import base64
import json
import re
from pprint import pprint
import os
import ssl
import urllib2
import urllib
from collections import defaultdict
from multiprocessing import Process

def keyExists(d, myKey): 
   return d.has_key(myKey) or any(myhaskey(dd) for dd in d.values() if isinstance(dd, dict))


def loadProperties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"') 
                props[key] = value 
    return props

def loadClassList(filepath, comment_char='#'):

   ciClassList = []

   with open(filepath, "rt") as f:
      for line in f:
         l = line.strip()
         if l and not l.startswith(comment_char):
            ciClassList.append(l)
   return(ciClassList)
 
#   ciClassList = { "cmdb_ci_cluster", "cmdb_ci_cluster_vip", "cmdb_ci_cluster_resource", "cmdb_ci_cluster_node", "cmdb_ci_vm", "cmdb_ci_server", "cmdb_ci_ip_router", "cmdb_ci_ip_switch", "cmdb_ci_appl", "cmdb_ci_db_instance", "cmdb_ci_service" }

def loadSnowServer(filepath, sep=',', comment_char='#'):

   ##########################################################################################
   #
   # This function reads the ServiceNow server configuration file and returns a dictionary
   #
   ##########################################################################################

   lineNum = 0
   with open(filepath, "rt") as f:
      for line in f:
         snowServerDict = {}
         l = line.strip()
         if l and not l.startswith(comment_char):
            values = l.split(sep)
            if(len(values) < 3):
               print "Malformed server configuration entry on line number: " + str(lineNum)
            else:
               snowServerDict["server"] = values[0]
               snowServerDict["user"] = values[1]
               snowServerDict["password"] = values[2]
         lineNum = lineNum + 1

   return(snowServerDict)

def verifyAsmConnectivity(asmDict):
 
   ##################################################################
   #
   # This function verifies that the ASM server credentials are valid
   # ---+++ CURRENTLY UNIMPLEMENTED +++---
   #
   ##################################################################

   return True

def loadEntityTypeMapping(filepath, sep=",", comment_char='#'):

   ################################################################################
   #
   # This function reads the entityType map configuration file and returns a dictionary
   #
   ################################################################################

   lineNum = 0

   with open(filepath, "rt") as f:
      for line in f:
         l = line.strip()
         if l and not l.startswith(comment_char):
            values = l.split(sep)
            if(len(values) < 2 or len(values) > 2):
               print "Malformed entityType map config line on line " + str(lineNum)
            else:
               entityTypeMappingDict[values[0].replace('"', '')] = values[1].replace('"', '')

def loadRelationshipMapping(filepath, sep=",", comment_char='#'):

   ################################################################################
   #
   # This function reads the relationship map configuration file and returns a dictionary
   #
   ################################################################################

   lineNum = 0
   #relationshipMappingDict = {}
   with open(filepath, "rt") as f:
      for line in f:
         l = line.strip()
         if l and not l.startswith(comment_char):
            values = l.split(sep)
            if(len(values) < 3 or len(values) > 3):
               print "Malformed mapping config line on line " + str(lineNum)
            else:
               relationshipMappingDict[values[0].replace('"', '')] = values[2].replace('"', '')

def loadAsmServer(filepath, sep=",", comment_char='#'):

   ################################################################################
   #
   # This function reads the ASM server configuration file and returns a dictionary
   #
   ################################################################################

   lineNum = 0
   with open(filepath, "rt") as f:
      for line in f:
         asmDict = {}
         l = line.strip()
         if l and not l.startswith(comment_char):
            values = l.split(sep)
            if(len(values) < 5):
               print "Malformed ASM server config line on line " + str(lineNum)
            else:
               asmDict["server"] = values[0]
               asmDict["port"] = values[1]
               asmDict["user"] = values[2]
               asmDict["password"] = values[3]
               asmDict["tenantid"] = values[4]
               if(verifyAsmConnectivity(asmDict)):
                  return(asmDict)
               else:
                  print "Unable to connect to ASM server " + asmDict["server"] + " on port " + asmDict["port"] + ", please verify server, username, password, and tenant id in " + mediatorHome + "config/asmserver.conf"
         
def createAsmRestListenJob(jobName):

   #####################################
   #
   # This function is currently not used
   #
   #####################################
   
   method = "POST"

   requestUrl = 'https://' + asmServerDict["server"] + ':' + asmServerDict["port"] + '/1.0/rest-observer/jobs/listen'

   jsonResource = '"unique_id":"HMC", "type": "listen", "parameters":{"provider": "HMC"}}'

   authHeader = 'Basic ' + base64.b64encode(asmServerDict["user"] + ":" + asmServerDict["password"])
   #print "auth header is: " + str(authHeader)
   #print "pushing the following json to ASM: " + jsonResource

   try:
      request = urllib2.Request(requestUrl, jsonResource)
      request.add_header("Content-Type",'application/json')
      request.add_header("Accept",'application/json')
      request.add_header("Authorization",authHeader)
      request.add_header("X-TenantId",asmServerDict["tenantid"])
      request.add_header("Provider","HMC")
      request.get_method = lambda: method

      response = urllib2.urlopen(request)
      xmlout = response.read()
      return True

   except IOError, e:
      print 'Failed to open "%s".' % requestUrl
      if hasattr(e, 'code'):
         print 'We failed with error code - %s.' % e.code
      elif hasattr(e, 'reason'):
         print "The error object has the following 'reason' attribute :"
         print e.reason
         print "This usually means the server doesn't exist,",
         print "is down, or we don't have an internet connection."
      return False

def createFileResource(resourceDict):

   #######################################################
   # 
   # Function to create a file observer entry for resource
   #
   #######################################################

   jsonResource = json.dumps(resourceDict)
   print "A:" + jsonResource

def createFileConnection(connectionDict):

   #########################################################
   # 
   # Function to create a file observer entry for connection 
   #
   #########################################################

   jsonResource = json.dumps(connectionDict)
   print "E:" + jsonResource

def createAsmResource(resourceDict):

   #######################################################
   #
   # Function to send a resource to the ASM rest interface
   #
   #######################################################

   method = "POST"

   #requestUrl = 'https://' + asmServerDict["server"] + ':' + asmServerDict["port"] + '/1.0/topology/resources'

   requestUrl = 'https://' + asmServerDict["server"] + ':' + asmServerDict["port"] + '/1.0/rest-observer/rest/resources'

   authHeader = 'Basic ' + base64.b64encode(asmServerDict["user"] + ":" + asmServerDict["password"])
   #print "auth header is: " + str(authHeader)
   jsonResource = json.dumps(resourceDict)
   #print "creating the following resource in ASM: " + jsonResource

   try:
      request = urllib2.Request(requestUrl, jsonResource)
      request.add_header("Content-Type",'application/json')
      request.add_header("Accept",'application/json')
      request.add_header("Authorization",authHeader)
      request.add_header("X-TenantId",asmServerDict["tenantid"])
      #request.add_header("JobId","HMC")
      request.get_method = lambda: method

      response = urllib2.urlopen(request)
      xmlout = response.read()
      return True

   except IOError, e:
      print 'Failed to open "%s".' % requestUrl
      if hasattr(e, 'code'):
         print 'We failed with error code - %s.' % e.code
      elif hasattr(e, 'reason'):
         print "The error object has the following 'reason' attribute :"
         print e.reason
         print "This usually means the server doesn't exist,",
         print "is down, or we don't have an internet connection."
      return False


def createAsmConnection(connectionDict):

   #########################################################
   #
   # Function to send a connection to the ASM rest interface
   #
   #########################################################
   
   method = "POST"

   requestUrl = 'https://' + asmServerDict["server"] + ':' + asmServerDict["port"] + '/1.0/rest-observer/rest/references'

   authHeader = 'Basic ' + base64.b64encode(asmServerDict["user"] + ":" + asmServerDict["password"])
   #print "auth header is: " + str(authHeader)
   jsonResource = json.dumps(connectionDict)
   #print "adding the following connection to ASM: " + jsonResource

   try:
      request = urllib2.Request(requestUrl, jsonResource)
      request.add_header("Content-Type",'application/json')
      request.add_header("Accept",'application/json')
      request.add_header("Authorization",authHeader)
      request.add_header("X-TenantId",asmServerDict["tenantid"])
      request.add_header("JobId","HMC")
      request.get_method = lambda: method

      response = urllib2.urlopen(request)
      xmlout = response.read()
      return True

   except IOError, e:
      print 'Failed to open "%s".' % requestUrl
      if hasattr(e, 'code'):
         print 'We failed with error code - %s.' % e.code
      elif hasattr(e, 'reason'):
         print "The error object has the following 'reason' attribute :"
         print e.reason
         print "This usually means the server doesn't exist,",
         print "is down, or we don't have an internet connection."
      return False

def getTotalRelCount():

   method = 'GET'
   requestUrl = 'https://' + snowServerDict["server"] + '/api/now/stats/cmdb_rel_ci?sysparm_count=true'
   print("issuing relationship count query: " + requestUrl)
   authHeader = 'Basic ' + base64.b64encode(snowServerDict["user"] + ":" + snowServerDict["password"])
     
   try:
      request = urllib2.Request(requestUrl)
      request.add_header("Content-Type",'application/json')
      request.add_header("Accept",'application/json')
      request.add_header("Authorization",authHeader)
      request.get_method = lambda: method

      response = urllib2.urlopen(request)
      relCountResult = response.read()

   except IOError, e:
      print 'Failed to open "%s".' % requestUrl
      if hasattr(e, 'code'):
         print 'We failed with error code - %s.' % e.code
      elif hasattr(e, 'reason'):
         print "The error object has the following 'reason' attribute :"
         print e.reason
         print "This usually means the server doesn't exist,",
         print "is down, or we don't have an internet connection."


   relCountResultDict = json.loads(relCountResult)
   print("Found " + relCountResultDict["result"]["stats"]["count"])
   return(int(relCountResultDict["result"]["stats"]["count"]))

 
def getCiData(runType, ciType):

   ###################################################
   #
   # query SNOW cmdb_ci table and generate ASM objects
   #
   ###################################################

   global ciSysIdSet
   global ciSysIdList
   global readCisFromFile

   readCiEntries = []
   writeToFile = 0
 
   readFromRest = 1

   if(readCisFromFile == "1"):
      if(os.path.isfile(mediatorHome  + "/log/" + ciType + ".json")):
         with open(mediatorHome + "/log/" + ciType + ".json") as text_file:
            completeResult = text_file.read()
            text_file.close()
         readCiEntries = json.loads(completeResult)
         del completeResult
         gc.collect()
         readFromRest = 0
      else:
         print "NOTE: read from file selected, yet file for ciType " + ciType + " does not exist. Reading from REST API."
         readFromRest = 1 
   else:
      readFromRest = 1

   if(readFromRest == 1):
      limit = 2500
      authHeader = 'Basic ' + base64.b64encode(snowServerDict["user"] + ":" + snowServerDict["password"])
      method = "GET"
      isMore = 1
      offset = 0
      firstRun = 1
   
      while(isMore):
   
         requestUrl = 'https://' + snowServerDict["server"] + '/api/now/table/' + ciType + '?sysparm_limit=' + str(limit) + '&sysparm_offset=' + str(offset)
         print 'issuing query: https://' + snowServerDict["server"] + '/api/now/table/' + ciType + '?sysparm_limit=' + str(limit) + '&sysparm_offset=' + str(offset)
     
         try:
            request = urllib2.Request(requestUrl)
            request.add_header("Content-Type",'application/json')
            request.add_header("Accept",'application/json')
            request.add_header("Authorization",authHeader)
            request.get_method = lambda: method
      
            response = urllib2.urlopen(request)
            ciDataResult = response.read()
      
         except IOError, e:
            print 'Failed to open "%s".' % requestUrl
            if hasattr(e, 'code'):
               print 'We failed with error code - %s.' % e.code
            elif hasattr(e, 'reason'):
               print "The error object has the following 'reason' attribute :"
               print e.reason
               print "This usually means the server doesn't exist,",
               print "is down, or we don't have an internet connection."
            return False
      
         #print "Result is: " + str(ciDataResult)
         ciEntries = json.loads(ciDataResult)
         for ci in ciEntries["result"]:
            #print "adding " + ci["name"] + " to readCiEntries..."
            readCiEntries.append(ci)
         numCi = len(ciEntries["result"])
         if(numCi < limit):
            #print "no more"
            isMore = 0
         else:
            #print "is more"
            offset = offset + limit
            isMore = 1
      
         print str(numCi) + " items in the cmdb ci table"

      writeToFile = 1 

   if(writeToFile):
      print "writing " + str(len(readCiEntries)) + " ci items to file"
      text_file = open(mediatorHome + "/log/" + ciType + ".json", "w")
      text_file.write(json.dumps(readCiEntries))
      text_file.close()
      
      
   #now, grab detail data for each sys_id found
   
   for ci in readCiEntries:

      # Ignore HP storage switches. This capability would probably be better as a config option, but no time for this POC to implement...

      if re.match(r"HP StorageWorks", ci["short_description"]) or re.match(r"HP P2000", ci["short_description"]) or re.match(r"HP MSA", ci["short_description"]):
         print "ignoring storage switch"
         continue
  
      asmObject = {}
      for prop in ci:
         if(ci[prop]):
            asmObject[prop] = ci[prop]
      if asmObject.has_key("name"):
         pass
      else:
         if(asmObject.has_key("ip_address")):
            asmObject["name"] = asmObject["ip_address"]
         elif asmObject.has_key("sys_id"):
            asmObject["name"] = asmObject["sys_id"]
      asmObject["_operation"] = "InsertReplace"
      asmObject["uniqueId"] = asmObject["sys_id"]
      if( asmObject["sys_class_name"] in entityTypeMappingDict):
         asmObject["entityTypes"] = [ entityTypeMappingDict[asmObject["sys_class_name"]] ]
      else:
         if(ciType in entityTypeMappingDict):
            asmObject["entityTypes"] = [ entityTypeMappingDict[ciType] ]
         else:
            print "no entitytype mapping for ciType: " + ciType + ", defaulting to 'server'"
            asmObject["entityTypes"] = "server"

      # Identify any fields that would be useful to use as matchTokens...

      asmObject["matchTokens"] = [ asmObject["name"] + ":" + asmObject["sys_id"] ]
      asmObject["matchTokens"].append( asmObject["sys_id"] )
      asmObject["matchTokens"].append( asmObject["name"] )
      if asmObject.has_key("ip_address"):
         if( asmObject["ip_address"] ):
            asmObject["matchTokens"].append(asmObject["ip_address"])
      if asmObject.has_key("dns_domain"):
         if(asmObject["dns_domain"]):
            asmObject["matchTokens"].append(asmObject["name"] + "." + asmObject["dns_domain"])
      if asmObject.has_key("os_domain"):
         if(asmObject["os_domain"]):
            asmObject["matchTokens"].append(asmObject["name"] + "." + asmObject["os_domain"])
      if asmObject.has_key("host_name"):
         if(asmObject["host_name"]):
            asmObject["matchTokens"].append(asmObject["host_name"]) 
            #print "changing name of ci object with name " + asmObject["name"] + " to the hostname: " + asmObject["host_name"]
            asmObject["name"] = asmObject["host_name"]
            #print "asm object with name: " + asmObject["name"] + " should have a matchToken that matches."
         


      ciList.append(asmObject)
      ciSysIdList.append(asmObject["sys_id"])


   print str(len(readCiEntries)) + " objects of type " + ciType + " found"
   del readCiEntries
   gc.collect()
   ciSysIdSet = set(ciSysIdList) # convert our ciSysIdList to a set for faster evaluation
   print "there are " + str(len(ciSysIdSet)) + " items in ciSysIdSet, while there are " + str(len(ciSysIdList)) + " items in ciCysIdList..."
   return()

def getCiRelationships():

   ###################################################
   #
   # query SNOW cmdb_rel table
   #
   ###################################################
   global readRelationshipsFromFile
   global totalSnowCmdbRelationships

   allRelEntries = []
   writeToFile = 1
   readFromRest = 0
   totalRelationships = 0

   if(readRelationshipsFromFile == "1"):
      writeToFile = 0
      print "Loading relationships from file rather than ServiceNow REST interface..."
      if(os.path.isfile(mediatorHome  + "/log/ciRelationships.json")):
         with open(mediatorHome + "/log/ciRelationships.json") as text_file:
            for relResult in text_file:
               #print(str(relResult))
               relEntries = json.loads(relResult)
               print "JPCLOG: FOUND " + str(len(relEntries["result"])) + " relationships for evaluation in this round of load."
               totalRelationships = totalRelationships + len(relEntries["result"])
               for rel in relEntries["result"]:
                  evaluateRelationship(rel)
         text_file.close()
         print "READ COMPLETE. Evaluated " + str(totalRelationships) + " relationships for relevance."
         readFromRest = 0
      else:
         print "NOTE: read from file selected, yet file for relationships does not exist. Obtaining relationships from REST API"
         readFromRest = 1
   else:
      print "reading relationships from ServiceNow REST API"
      readFromRest = 1
  
   if(readFromRest == 1):  
 
      limit = 50000
      authHeader = 'Basic ' + base64.b64encode(snowServerDict["user"] + ":" + snowServerDict["password"])
      method = "GET"
      isMore = 1
      offset = 0
      relPass = 1
      retryQuery = 0
      totalRelationshipsEvaluated = 0
   
      while(isMore):
 
         if(retryQuery > 0):
            if(retryQuery == 4):
               print("FATAL: Query at position: " + offset + " has failed 4 times. Skipping this segment...")
               offset = offset + limit
            else:
               print "retrying query due to read failure"
  
         requestUrl = 'https://' + snowServerDict["server"] + '/api/now/table/cmdb_rel_ci?sysparm_limit=' + str(limit) + '&sysparm_offset=' + str(offset)
         print("issuing query: " + requestUrl)
     
         for retry in [1,2,3]:
            try:
               request = urllib2.Request(requestUrl)
               request.add_header("Content-Type",'application/json')
               request.add_header("Accept",'application/json')
               request.add_header("Authorization",authHeader)
               request.get_method = lambda: method
         
               response = urllib2.urlopen(request)
               relDataResult = response.read()
               break
         
            except (IOError, IncompleteRead), e:
               print 'Failed to open "%s".' % requestUrl
               if hasattr(e, 'code'):
                  print 'We failed with error code - %s.' % e.code
               elif hasattr(e, 'reason'):
                  print "The error object has the following 'reason' attribute :"
                  print e.reason
                  print "This usually means the server doesn't exist,",
                  print "is down, or we don't have an internet connection."
               else:
                  print("ERROR: Unable to read from URL: " + requestUrl)
               if(retry == 3):
                  print("FATAL: READ ERROR AFTER 3 TRIES: ABORTING READ")
                  print("Aborted URL: " + requestUrl)
                  exit()

         try:
            relEntries = json.loads(relDataResult)
            retryQuery = 0
         except ValueError:
            print("ERROR: JSON parsing failed. Retrying query")
            retryQuery = retryQuery + 1

         if(retryQuery == 0):          # Successful read/load. No need to retry

            if(writeToFile):
               print "Saving " + str(len(relEntries["result"])) + " relationship items to file for future load"
               if(relPass == 1):
                  text_file = open(mediatorHome + "/log/ciRelationships.json", "w")
               else:
                  text_file = open(mediatorHome + "/log/ciRelationships.json", "a")
               text_file.write(relDataResult)
               text_file.write("\n")
               text_file.close()

            totalRelationshipsEvaluated = totalRelationshipsEvaluated + len(relEntries["result"])
   
            print "evaluating " + str(len(relEntries["result"])) + " relationships in this pass"
            for rel in relEntries["result"]:
               evaluateRelationship(rel)

            numRel = len(relEntries["result"])
   
            if(numRel < limit):
               if(totalRelationshipsEvaluated <= (totalSnowCmdbRelationships - (limit * 2))):
                  print("suspected short read... retrying")
                  #offset = offset + limit
                  #relPass = relPass + 1
                  retryQuery = retryQuery + 1
               else:
                  isMore = 0
            else:
               offset = offset + limit
               isMore = 1
               relPass = relPass + 1
               retryQuery = 0
      
         #print str(numRel) + " items in the cmdb relationships table"

      print "Relationship evaluation complete. Evaluated a total of " + str(totalRelationshipsEvaluated) + " relationships out of an expected " + str(totalSnowCmdbRelationships)


#   if(writeToFile):
#      print "writing " + str(len(allRelEntries)) + " relationship items to file"
#      text_file = open(mediatorHome + "/log/ciRelationships.json", "w")
#      text_file.write(json.dumps(allRelEntries))
#      text_file.close()
 
   #print "cycling through " + str(len(allRelEntries)) + " relationship entries"
   #numRels = len(allRelEntries)
   #relCount = 0
   #print "there are " + str(len(ciSysIdSet)) + " items in ciSysIdSet, while there are " + str(len(ciSysIdList)) + " items in ciCysIdList..."



def evaluateRelationship(rel):

   global ciSysIdSet
   global relationList
   #global allRelEntries

   relevant = 0
   
   if(isinstance(rel, dict)):
      pass
   else:
      print("relation passed to evaluateRelationship is not a dictionary. It contains the following:")
      print(str(rel))
      return

   if(rel.has_key("child") and rel.has_key("parent")):
      pass
   else:
      return

   if(isinstance(rel["child"], dict) and isinstance(rel["parent"], dict)):
      #print "found connection with both parent and child."
      #print "Parent is: " + rel["parent"]["value"]
      #print "Child is: " + rel["child"]["value"]
      #print "evaluating ciSysIdSet to see if both child/parent is there. Length of ciSysIdSet is: " + str(len(ciSysIdSet))
      if(rel["child"]["value"] in ciSysIdSet and rel["parent"]["value"] in ciSysIdSet):
         #if(str(rel["parent"]) in ciSysIdList):
         if 1==1:
            #print "===== both parent and child in ciSysIdList, i.e. in topology, saving relationship. Parent: " + str(rel["parent"]["value"]) + ", Child: " + str(rel["child"]["value"])
            if( rel["type"]["value"] in relationshipMappingDict):
               thisRelType = relationshipMappingDict[ rel["type"]["value"] ]
            else:
               print "unmapped relationship type: " + rel["type"]["value"] + ". Using default 'connectedTo'."
               thisRelType = "connectedTo"
            relationDict = { "_fromUniqueId": rel["parent"]["value"], "_toUniqueId": rel["child"]["value"], "_edgeType": thisRelType }
            relationDict["originalRelSysId"] = rel["type"]["value"]
            if rel["type"]["value"] not in relTypeSet:
               relTypeSet.add(rel["type"]["value"])
            #print "found a relevant connection... adding to relationList array..."
            relationList.append(relationDict)
         else:
            pass
      else:
         pass
         #print "neither parent or child is in siSysIdList, discarding"
   else:
      pass
      # either parent or child designated is not returning dict for this entry

def getCiDetail(sys_id, ciType):

   ###############################################################
   #
   # This function grabs ci detail data based on sys_id and ciType
   # ---+++ CURRENTLY UNIMPLEMENTED +++---
   #
   ###############################################################

 
   authHeader = 'Basic ' + base64.b64encode(snowServerDict["user"] + ":" + snowServerDict["password"])

   method = "GET"

   requestUrl = 'https://' + snowServerDict["server"] + '/api/now/cmdb/instance/' + ciType + "/" + sys_id + "?sysparm_limit=10000"


   try:
      request = urllib2.Request(requestUrl)
      request.add_header("Content-Type",'application/json')
      request.add_header("Accept",'application/json')
      request.add_header("Authorization",authHeader)
      request.get_method = lambda: method

      response = urllib2.urlopen(request)
      ciDetailResult = response.read()

   except IOError, e:
      print 'Failed to open "%s".' % requestUrl
      if hasattr(e, 'code'):
         print 'We failed with error code - %s.' % e.code
      elif hasattr(e, 'reason'):
         print "The error object has the following 'reason' attribute :"
         print e.reason
         print "This usually means the server doesn't exist,",
         print "is down, or we don't have an internet connection."
      return False

   ##print "logging out..."

   #print ciDetailResult
   ciEntry = json.loads(ciDetailResult)

   for relation in ciEntry["result"]["inbound_relations"]:
      createCiRelationship(sys_id, relation, "inbound")

   for relation in ciEntry["result"]["outbound_relations"]:
      createCiRelationship(sys_id, relation, "outbound")

   ciObject = ciEntry["result"]["attributes"]
#   ciObject["uniqueId"] = 
#   ciObject["name"] = 
#   cnaDict["entityTypes"] = 

   return(ciObject)


def createCiRelationship(sys_id, relationDict, relationDir):

   relationType = relationDict["type"]["display_value"]
   if relationType in relationshipMappingDict:
      edgeType = relationshipMappingDict[relationType]
   else:
      edgeType = "connectedTo"
      print "Unmapped relationship type " + relationType + ", using connectedTo by default" 
   
   if(relationDir == "inbound"):
      relationDict = { "_fromUniqueId": relationDict["target"]["value"], "_toUniqueId": sys_id, "_edgeType": edgeType}
      relationList.append(relationDict)
   elif(relationDir == "outbound"):
      relationDict = { "_fromUniqueId": sys_id, "_toUniqueId": relationDict["target"]["value"], "_edgeType": edgeType}
      relationList.append(relationDict)
   
   

#   cnaDict["uniqueId"] = id
#   cnaDict["name"] = cnaDict["MACAddress"]
#   cnaDict["entityTypes"] = [ "networkinterface" ]

   return cnaDict

######################################
#
#  ----   Main multiprocess dispatcher
#
######################################

if __name__ == '__main__':

   # messy global definitions in the interest of saving time..........

   global mediatorHome
   global logHome
   global configHome
   configDict = {}
   global asmDict
   asmDict = {}
   global snowServerDict
   global relationshipMappingDict
   relationshipMappingDict = {}
   global ciList
   ciList = []
   global ciSysIdList
   ciSysIdList = []
   global ciSysIdSet
   ciSysIdSet = set()
   global relationList
   relationList = []
   global entityTypeMappingDict
   entityTypeMappingDict = {}
   global writeToFile
   global relTypeSet
   relTypeSet = set() 
   global totalSnowCmdbRelationships


   ############################################
   #
   # verify directories and load configurations
   #
   ############################################

   mediatorBinDir = os.path.dirname(os.path.abspath(__file__))
   extr = re.search("(.*)bin", mediatorBinDir)
   if extr:
      mediatorHome = extr.group(1)
      #print "Mediator home is: " + mediatorHome
   else:
      print "FATAL: unable to find mediator home directory. Is it installed properly? bindir = " + mediatorBinDir
      exit()

   if(os.path.isdir(mediatorHome + "log")):
      logHome = extr.group(1)
   else:
      print "FATAL: unable to find log directory at " + mediatorHome + "log"
      exit()

   if(os.path.isfile(mediatorHome + "/config/snowserver.conf")):
      snowServerDict = loadSnowServer(mediatorHome + "/config/snowserver.conf")
   else:
      print "FATAL: unable to find ServiceNow server list file " + mediatorHome + "/config/snowserver.conf"
      exit()

   if(os.path.isfile(mediatorHome + "/config/classlist.conf")):
      ciClassList = loadClassList(mediatorHome + "/config/classlist.conf")
   else:
      print "FATAL: unable to find ServiceNow server list file " + mediatorHome + "/config/snowserver.conf"
      exit()

#   if(os.path.isfile(mediatorHome + "/config/asmserver.conf")):
#      asmServerDict = loadAsmServer(mediatorHome + "/config/asmserver.conf")
#   else:
#      print "FATAL: unable to find ASM server configuration file " + mediatorHome + "/config/asmserver.conf"
#      exit()

   if(os.path.isfile(mediatorHome  + "/config/relationship-mapping.conf")):
      relationshipMapping = loadRelationshipMapping(mediatorHome + "/config/relationship-mapping.conf")
   else:
      print "FATAL: no relationship mapping file available at " + mediatorHome + "/config/relationship-mapping.conf"

   if(os.path.isfile(mediatorHome  + "/config/entitytype-mapping.conf")):
      relationshipMapping = loadEntityTypeMapping(mediatorHome + "/config/entitytype-mapping.conf")
   else:
      print "FATAL: no entity type mapping file available at " + mediatorHome + "/config/entitytype-mapping.conf"

   if(os.path.isfile(mediatorHome  + "/config/getSNOWData.props")):
      configVars = loadProperties(mediatorHome + "/config/getSNOWData.props")
      print str(configVars)
      if 'readCisFromFile' in configVars.keys():
         global readCisFromFile
         readCisFromFile = configVars['readCisFromFile']
         if(readCisFromFile == "1"):
            print "will read CIs from file if available"
         else:
            print "will read CIs from ServiceNow REST API"
      else:
         print "readCisFromFile property not set, defaulting to 0, read from ServiceNow REST API"
         readCisFromFile = 0
      if 'readRelationshipsFromFile' in configVars.keys():
         global readRelationshipsFromFile
         readRelationshipsFromFile = configVars['readRelationshipsFromFile']
         if(readRelationshipsFromFile == "1"):
            print "will read relationships from file if available"
         else:
            print "will read relationships from ServiceNow REST API"
      else:
         print("readRelationshipsFromFile not in properties file, defaulting to 0, read from ServiceNow REST API")
         readRelationshipsFromFile = 0
   else:
      print "FATAL: unable to find the properties file " + mediatorHome + "/config/getSNOWData.props"

#   print("readCisFromFile is: " + str(readCisFromFile))
#   print("readRelationshipsFromFile is: " + str(readRelationshipsFromFile))

   ############################################################################
   #
   # Cycle through each class of interest, and obtain CI records for each class
   #
   ############################################################################

   startTime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
   print("Start time: " + startTime)
   for className in ciClassList:
      print "querying SNOW for all CIs of type " + className
      getCiData("pre", className)

   print "CI mediation complete. Writing vertices file..."   
   vertices = open(mediatorHome + "/file-observer-files/vertices-" + str(datetime.datetime.now()) + ".json", "w")
   for ci in ciList:
      ci_text = json.dumps(ci)
      vertices.write("V:" + ci_text + "\n" + "W:5 millisecond" + "\n")
      vertices.flush()
   vertices.close()
   totalCi = len(ciList)
   del ciList
   gc.collect()
   

   ###################################################################################################################################
   #
   # Next, we pull the relationship table. Then we will evaluate relationships that only are pertinent to our CIs of interest.
   # Both CI's of a relationship must be in our CI Class list for inclusion in topology. Any other relationship is deemed irrelevant
   # and discarded.
   #
   ###################################################################################################################################

   print "Loading and evaluating relationship table"
   totalSnowCmdbRelationships = getTotalRelCount()
   getCiRelationships()

## multi-processing stuff that isn't in use currently. could be enabled to concurrently pull all data for CIs and relationships to speed up the mediator
#      p = Process(target=getCiData, args=(className,))
#      p.start()

#   for ci in ciList:
#      print ci["name"]



   # Currently, this mediator only writes out file observer files. There are functions defined above that can directly inject into ASM via REST, but not in use at this time.
   # e.g. "createAsmResource() can be used to send a ci dict in ciList directly to the ASM rest interface
 
      
   print "Relationship mediation complete. Writing edges..."   
   edges = open(mediatorHome + "/file-observer-files/edges-" + str(datetime.datetime.now()) + ".json", "w")
   for rel in relationList:
      ci_text = json.dumps(rel)
      edges.write("E:" + ci_text + "\n" + "W:5 millisecond" + "\n")
      edges.flush()
   totalRelation = len(relationList)
   del relationList
   gc.collect()
   edges.close()
      
   print "Mediation complete."
   print "Number of CIs: " + str(totalCi)
   print "Number of Relations: " + str(totalRelation)
   print "Mediation started at: " + startTime
   print("Mediation completed at: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

   #debug info

   ## These functions may be used to send CI and relationships directly to ASM REST interface:
   # UNTESTED - this is code from the HMC mediation code available here: https://github.ibm.com/jcress/HMC-Mediator-for-Agile-Service-Manager

   #for ci in ciList:
   #   createAsmResource(ci)

   #for rel in relationList:
   #   createAsmConnection(rel)

   print "Unique relation types:"
   for relType in relTypeSet:
      print relType
      
      

      
   print "all done"

   exit()

   # debug info

   for ci in ciList:
      print ci["name"]
      print "======================="
   exit()


