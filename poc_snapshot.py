#! /usr/bin/env python3
pocUtilsVersion = '2.3.0'

#--python imports
import argparse
import csv
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta

try: import configparser
except: import ConfigParser as configparser

#--senzing python classes
try: from G2Database import G2Database
except:
    print('')
    print('Please export PYTHONPATH=<path to senzing python directory>')
    print('')
    sys.exit(1)

#--see if a g2 config manager present - v1.12+
try: 
    from G2IniParams import G2IniParams
    from G2ConfigMgr import G2ConfigMgr
except: G2ConfigMgr = None

#---------------------------------------
def processEntities():
    global shutDown

    print('Querying entities ...')
    cursor1 = g2Dbo.sqlExec('select max(RES_ENT_ID) as MAX_RES_ENT_ID from RES_ENT')
    rowData = g2Dbo.fetchNext(cursor1)
    maxResEntId = rowData['MAX_RES_ENT_ID'] if rowData else None
    if not maxResEntId:
        return 1

    sql = 'select '
    sql += ' a.RES_ENT_ID, ' 
    sql += ' a.ERRULE_ID, ' 
    sql += ' a.MATCH_KEY, ' 
    sql += ' b.DSRC_ID, '
    sql += ' c.RECORD_ID '
    sql += 'from RES_ENT_OKEY a '
    sql += 'join OBS_ENT b on b.OBS_ENT_ID = a.OBS_ENT_ID '
    sql += 'join DSRC_RECORD c on c.ENT_SRC_KEY = b.ENT_SRC_KEY and c.DSRC_ID = b.DSRC_ID and c.ETYPE_ID = b.ETYPE_ID '
    sql += 'where a.RES_ENT_ID between ? and ? '
    sql += 'order by a.RES_ENT_ID '

    statPack['TOTAL_RECORD_COUNT'] = 0
    statPack['TOTAL_ENTITY_COUNT'] = 0
    statPack['DATA_SOURCES'] = {}
    statPack['ENTITY_SIZE_BREAKDOWN'] = {}

    #--start processing rows
    begEntityId = 1
    endEntityId = chunkSize
    lastResEntId = 0
    entityCount = 0
    batchStartTime = time.time()
    while True:
        print('Getting entities from %s to %s ...' % (begEntityId, endEntityId))
        cursor1 = g2Dbo.sqlExec(sql, (begEntityId, endEntityId))
        rowData = g2Dbo.fetchNext(cursor1)
        if not rowData and lastResEntId >= maxResEntId:
            print(' No more found, processing complete!')
            break
        while rowData:
            entityCount += 1
            randomSampleI = random.randint(1,99)

            #--create record summary
            entitySize = 0
            entityData = {}
            entityData['ENTITY_ID'] = rowData['RES_ENT_ID']
            entityData['RECORD_SUMMARY'] = {}
            while rowData and rowData['RES_ENT_ID'] == entityData['ENTITY_ID']:
                entitySize += 1
                try: rowData['DATA_SOURCE'] = dsrcLookup[rowData['DSRC_ID']]['DSRC_CODE']
                except: rowData['DATA_SOURCE'] = 'unk'
                if rowData['DATA_SOURCE'] not in entityData['RECORD_SUMMARY']:
                    entityData['RECORD_SUMMARY'][rowData['DATA_SOURCE']] = 1
                else:
                    entityData['RECORD_SUMMARY'][rowData['DATA_SOURCE']] += 1
                if exportFilePath:
                    try: rowData['DATA_SOURCE'] = dsrcLookup[rowData['DSRC_ID']]['DSRC_CODE']
                    except: rowData['DATA_SOURCE'] = 'unk'
                    try: rowData['MATCH_LEVEL'] = erruleLookup[rowData['ERRULE_ID']]['RTYPE_ID']
                    except: rowData['MATCH_LEVEL'] = 0
                    columnValues = []
                    columnValues.append(str(rowData['RES_ENT_ID']))
                    columnValues.append('0') #--related entity_id
                    columnValues.append(str(rowData['MATCH_LEVEL']))
                    columnValues.append(rowData['MATCH_KEY'][1:] if rowData['MATCH_KEY'] else '')
                    columnValues.append(rowData['DATA_SOURCE'])
                    columnValues.append(rowData['RECORD_ID'])
                    try: exportFileHandle.write(','.join(columnValues) + '\n')        
                    except IOError as err: 
                        print('')
                        print('ERROR: cannot write to %s \n%s' % (exportFilePath, err))
                        print('')
                        shutDown = True
                        break

                rowData = g2Dbo.fetchNext(cursor1)
            statPack['TOTAL_ENTITY_COUNT'] += 1
            lastResEntId = entityData['ENTITY_ID']

            #--update entity size breakdown
            strEntitySize = str(entitySize)
            if strEntitySize not in statPack['ENTITY_SIZE_BREAKDOWN']:
                statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize] = {}
                statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['COUNT'] = 0
                statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'] = []
            statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['COUNT'] += 1
            if len(statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE']) < sampleSize:
                statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'].append(entityData['ENTITY_ID'])
            elif randomSampleI % 10 != 0:
                statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'][randomSampleI] = entityData['ENTITY_ID']

            #--update data source breakdown
            for dataSource in entityData['RECORD_SUMMARY']:
                statPack['TOTAL_RECORD_COUNT'] += entityData['RECORD_SUMMARY'][dataSource]

                #--basic data source stats
                if dataSource not in statPack['DATA_SOURCES']:
                    statPack['DATA_SOURCES'][dataSource] = {}
                    statPack['DATA_SOURCES'][dataSource]['RECORD_COUNT'] = entityData['RECORD_SUMMARY'][dataSource]
                    statPack['DATA_SOURCES'][dataSource]['ENTITY_COUNT'] = 1
                    statPack['DATA_SOURCES'][dataSource]['SINGLE_COUNT'] = 0
                    statPack['DATA_SOURCES'][dataSource]['SINGLE_SAMPLE'] = []
                    statPack['DATA_SOURCES'][dataSource]['DUPLICATE_ENTITY_COUNT'] = 0
                    statPack['DATA_SOURCES'][dataSource]['DUPLICATE_RECORD_COUNT'] = 0
                    statPack['DATA_SOURCES'][dataSource]['DUPLICATE_SAMPLE'] = []
                    statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'] = {}
                else:
                    statPack['DATA_SOURCES'][dataSource]['RECORD_COUNT'] += entityData['RECORD_SUMMARY'][dataSource]
                    statPack['DATA_SOURCES'][dataSource]['ENTITY_COUNT'] += 1

                #--singletons or multiples
                if entityData['RECORD_SUMMARY'][dataSource] == 1:
                    statPack['DATA_SOURCES'][dataSource]['SINGLE_COUNT'] += 1
                    if len(statPack['DATA_SOURCES'][dataSource]['SINGLE_SAMPLE']) < sampleSize:
                        statPack['DATA_SOURCES'][dataSource]['SINGLE_SAMPLE'].append(entityData['ENTITY_ID'])
                    elif randomSampleI % 10 != 0:
                        statPack['DATA_SOURCES'][dataSource]['SINGLE_SAMPLE'][randomSampleI] = entityData['ENTITY_ID']
                else:
                    statPack['DATA_SOURCES'][dataSource]['DUPLICATE_ENTITY_COUNT'] += 1
                    statPack['DATA_SOURCES'][dataSource]['DUPLICATE_RECORD_COUNT'] += entityData['RECORD_SUMMARY'][dataSource]
                    if len(statPack['DATA_SOURCES'][dataSource]['DUPLICATE_SAMPLE']) < sampleSize:
                        statPack['DATA_SOURCES'][dataSource]['DUPLICATE_SAMPLE'].append(entityData['ENTITY_ID'])
                    elif randomSampleI % 10 != 0:
                        statPack['DATA_SOURCES'][dataSource]['DUPLICATE_SAMPLE'][randomSampleI] = entityData['ENTITY_ID']

                #--cross matches
                for dataSource1 in entityData['RECORD_SUMMARY']:
                    if dataSource1 != dataSource:

                        if dataSource1 not in statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES']:
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1] = {}
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_RECORD_COUNT'] = entityData['RECORD_SUMMARY'][dataSource]
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_ENTITY_COUNT'] = 1
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_SAMPLE'] = []
                        else:
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_RECORD_COUNT'] += entityData['RECORD_SUMMARY'][dataSource]
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_ENTITY_COUNT'] += 1

                        if len(statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_SAMPLE']) < sampleSize:
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_SAMPLE'].append(entityData['ENTITY_ID'])
                        elif randomSampleI % 10 != 0:
                            statPack['DATA_SOURCES'][dataSource]['CROSS_MATCHES'][dataSource1]['MATCH_SAMPLE'][randomSampleI] = entityData['ENTITY_ID']

            #--status display
            if entityCount % progressInterval == 0 or not rowData:
                now = datetime.now().strftime('%I:%M%p').lower()
                elapsedMins = round((time.time() - procStartTime) / 60, 1)
                eps = int(float(progressInterval) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
                batchStartTime = time.time()
                if rowData:
                    print(' %s entities processed at %s, %s per second' % (entityCount, now, eps))
                else:
                    print(' %s entities completed at %s after %s minutes' % (entityCount, now, elapsedMins))

            #--get out if errors hit or out of records
            if shutDown or not rowData:
                break

        #--get out if errors hit
        if shutDown:
            break
        else: #--set next batch
            begEntityId += chunkSize
            endEntityId += chunkSize

    #--get out if errors hit
    if shutDown:
        return 1

    #--calculate some percentages
    statPack['TOTAL_COMPRESSION'] = str(round(100.00-((float(statPack['TOTAL_ENTITY_COUNT']) / float(statPack['TOTAL_RECORD_COUNT'])) * 100.00), 2)) + '%'
    for dataSource in statPack['DATA_SOURCES']:
        statPack['DATA_SOURCES'][dataSource]['COMPRESSION'] = str(round(100.00-((float(statPack['DATA_SOURCES'][dataSource]['ENTITY_COUNT']) / float(statPack['DATA_SOURCES'][dataSource]['RECORD_COUNT'])) * 100.00), 2)) + '%'

    if False: #--attempt to only count 1 per obs_ent is too slow
        featureSql = 'select '
        featureSql += ' a.OBS_ENT_ID, '
        featureSql += ' b.FTYPE_ID, '
        featureSql += ' b.LIB_FEAT_ID, '
        featureSql += ' c.SUPPRESSED '
        featureSql += 'from RES_ENT_OKEY a '
        featureSql += 'join OBS_FEAT_EKEY b on b.OBS_ENT_ID = a.OBS_ENT_ID '
        featureSql += 'join RES_FEAT_EKEY c on c.RES_ENT_ID = a.RES_ENT_ID and c.LENS_ID = a.LENS_ID and c.LIB_FEAT_ID = b.LIB_FEAT_ID and c.UTYPE_CODE = b.UTYPE_CODE '
        featureSql += 'where a.RES_ENT_ID = ? and a.LENS_ID = 1 ' #--and b.FTYPE_ID < 15'
    else:
        featureSql = 'select '
        featureSql += ' a.RES_ENT_ID as OBS_ENT_ID, '
        featureSql += ' a.FTYPE_ID, '
        featureSql += ' a.LIB_FEAT_ID, '
        featureSql += ' a.SUPPRESSED '
        featureSql += 'from RES_FEAT_EKEY a '
        featureSql += 'where a.RES_ENT_ID = ? and a.LENS_ID = 1 and a.FTYPE_ID < 15'

    #--add feature stats to the entity size break down
    entitySizeBreakdown = {}
    for strEntitySize in statPack['ENTITY_SIZE_BREAKDOWN']:
        entitySize = int(strEntitySize)
        if entitySize < 10:
            entitySizeLevel = entitySize
        elif entitySize < 100:
            entitySizeLevel = int(entitySize/10) * 10
        else:
            entitySizeLevel = int(entitySize/100) * 100

        if entitySizeLevel not in entitySizeBreakdown:
            entitySizeBreakdown[entitySizeLevel] = {}
            entitySizeBreakdown[entitySizeLevel]['ENTITY_COUNT'] = 0
            entitySizeBreakdown[entitySizeLevel]['SAMPLE_ENTITIES'] = []
            entitySizeBreakdown[entitySizeLevel]['REVIEW_COUNT'] = 0
            entitySizeBreakdown[entitySizeLevel]['REVIEW_REASONS'] = {}
        entitySizeBreakdown[entitySizeLevel]['ENTITY_COUNT'] += statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['COUNT']

        for entityID in statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE']:
            entitySizeBreakdown[entitySizeLevel]['SAMPLE_ENTITIES'].append(entityID)

            #--gather feature statistics
            if entitySize > 1:

                if False: #--attempt to only count 1 per obs_ent is too slow
                    featureData = {}
                    cursor1 = g2Dbo.sqlExec(featureSql, (entityID,))
                    rowData = g2Dbo.fetchNext(cursor1)
                    while rowData:
                        if rowData['SUPPRESSED'] != 'Y' and ftypeLookup[rowData['FTYPE_ID']]['DERIVED'].upper().startswith('N'):
                            ftypeID = rowData['FTYPE_ID']
                            libFeatID = rowData['LIB_FEAT_ID']
                            obsEntID = rowData['OBS_ENT_ID']
                            if ftypeID not in featureData:
                                featureData[ftypeID] = {}
                            if libFeatID not in featureData[ftypeID]:
                                featureData[ftypeID][libFeatID] = []
                            featureData[ftypeID][libFeatID].append(obsEntID)
                        rowData = g2Dbo.fetchNext(cursor1)

                    #--only count each distinct feature once per obs_ent 
                    #--keeps from counting two features for the same obs_ent
                    featureStats = {}
                    for ftypeID in featureData:
                        if len(featureData[ftypeID]) > 1: #--bypass any features with only one value
                            featureStats[ftypeID] = 0
                            obsEntList = []
                            for libFeatID in featureData[ftypeID]:
                                countIt = False
                                for obsEntID in featureData[ftypeID][libFeatID]:
                                    if obsEntID not in obsEntList:
                                        obsEntList.append(obsEntID)
                                        countIt = True
                                if countIt:
                                    featureStats[ftypeID] += 1

                #--streamlined method
                else: 
                    featureStats = {}
                    cursor1 = g2Dbo.sqlExec(featureSql, (entityID,))
                    rowData = g2Dbo.fetchNext(cursor1)
                    while rowData:
                        if rowData['SUPPRESSED'] != 'Y' and ftypeLookup[rowData['FTYPE_ID']]['DERIVED'].upper().startswith('N'):
                            ftypeID = rowData['FTYPE_ID']
                            if ftypeID not in featureStats:
                                featureStats[ftypeID] = 1
                            else:
                                featureStats[ftypeID] += 1
                        rowData = g2Dbo.fetchNext(cursor1)                        

                if entitySize <= 5: #--super small
                    maxExclusiveCnt = 1
                    maxNameCnt = 5
                    maxAddrCnt = 5
                    maxF1Cnt = 3
                    maxFFCnt = 5
                elif entitySize <= 50: #--medium
                    maxExclusiveCnt = 1
                    maxNameCnt = 10
                    maxAddrCnt = 10
                    maxF1Cnt = 10
                    maxFFCnt = 10
                else: #--large
                    maxExclusiveCnt = 1
                    maxNameCnt = 25
                    maxAddrCnt = 25
                    maxF1Cnt = 25
                    maxFFCnt = 25

                reviewFeatures = []
                for ftypeID in featureStats:
                    ftypeCode = ftypeLookup[ftypeID]['FTYPE_CODE']
                    frequency = ftypeLookup[ftypeID]['FTYPE_FREQ']
                    exclusive = str(ftypeLookup[ftypeID]['FTYPE_EXCL']).upper() in ('1', 'Y', 'YES')
                    if exclusive and featureStats[ftypeID] > maxExclusiveCnt:
                        reviewFeatures.append(ftypeCode)
                        #print(ftypeCode, featureStats[ftypeID])
                    elif ftypeCode == 'NAME' and featureStats[ftypeID] > maxNameCnt:
                        reviewFeatures.append(ftypeCode)
                        #print(ftypeCode, featureStats[ftypeID])
                    elif ftypeCode == 'ADDRESS' and featureStats[ftypeID] > maxAddrCnt:
                        reviewFeatures.append(ftypeCode)
                        #print(ftypeCode, featureStats[ftypeID])
                    elif frequency == 'F1' and featureStats[ftypeID] > maxF1Cnt:
                        reviewFeatures.append(ftypeCode)
                        #print(ftypeCode, featureStats[ftypeID])
                    elif frequency == 'FF' and featureStats[ftypeID] > maxFFCnt:
                        #print(ftypeCode, featureStats[ftypeID])
                        reviewFeatures.append(ftypeCode)

                if reviewFeatures:
                    #print(entityID)
                    #pause()

                    entitySizeBreakdown[entitySizeLevel]['REVIEW_COUNT'] += 1
                    reviewReason = '+'.join(sorted(reviewFeatures))
                    if reviewReason not in entitySizeBreakdown[entitySizeLevel]['REVIEW_REASONS']:
                        entitySizeBreakdown[entitySizeLevel]['REVIEW_REASONS'][reviewReason] = []
                    entitySizeBreakdown[entitySizeLevel]['REVIEW_REASONS'][reviewReason].append(entityID)

    statPack['ENTITY_SIZE_BREAKDOWN'] = []
    for entitySize in sorted(entitySizeBreakdown.keys()):
        entitySizeRecord = entitySizeBreakdown[entitySize]
        entitySizeRecord['ENTITY_SIZE'] = int(entitySize)
        entitySizeRecord['ENTITY_SIZE_DISPLAY'] = str(entitySize) + ('+' if int(entitySize) >= 10 else '')
        statPack['ENTITY_SIZE_BREAKDOWN'].append(entitySizeRecord)

    return 0

#---------------------------------------
def processRelationships():
    global shutDown

    #--in case processEntities was skipped
    if 'DATA_SOURCES' not in statPack:
        statPack['DATA_SOURCES'] = {}

    print('Querying relationships ...')
    cursor1 = g2Dbo.sqlExec('select max(RES_REL_ID) as MAX_RES_REL_ID from RES_RELATE')
    rowData = g2Dbo.fetchNext(cursor1)
    maxResRelId = rowData['MAX_RES_REL_ID'] if rowData else None 
    if not maxResRelId:
        print('No relationships found')
        return 0 #--this is not an error condition!

    sql = 'select * from '
    sql += '( '
    sql += 'select '
    sql += ' a.RES_REL_ID, '
    sql += ' a.MIN_RES_ENT_ID, ' 
    sql += ' a.MAX_RES_ENT_ID, ' 
    sql += ' a.LAST_ERRULE_ID, ' 
    sql += ' a.MATCH_KEY, ' 
    sql += ' a.IS_DISCLOSED, '
    sql += ' a.IS_AMBIGUOUS, '
    sql += ' b.RES_ENT_ID, '
    sql += ' c.DSRC_ID, '
    sql += ' d.RECORD_ID '
    sql += 'from RES_RELATE a, RES_ENT_OKEY b, OBS_ENT c, DSRC_RECORD d '
    sql += 'where b.RES_ENT_ID = a.MIN_RES_ENT_ID '
    sql += 'and c.OBS_ENT_ID = b.OBS_ENT_ID '
    sql += 'and d.ENT_SRC_KEY = c.ENT_SRC_KEY and d.DSRC_ID = c.DSRC_ID and d.ETYPE_ID = c.ETYPE_ID '
    sql += 'and a.RES_REL_ID between ? and ? '
    sql += ' union '
    sql += 'select '
    sql += ' a.RES_REL_ID, '
    sql += ' a.MIN_RES_ENT_ID, ' 
    sql += ' a.MAX_RES_ENT_ID, ' 
    sql += ' a.LAST_ERRULE_ID, '
    sql += ' a.MATCH_KEY, '
    sql += ' a.IS_DISCLOSED, '
    sql += ' a.IS_AMBIGUOUS, '
    sql += ' b.RES_ENT_ID, '
    sql += ' c.DSRC_ID, '
    sql += ' d.RECORD_ID '
    sql += 'from RES_RELATE a, RES_ENT_OKEY b, OBS_ENT c, DSRC_RECORD d '
    sql += 'where b.RES_ENT_ID = a.MAX_RES_ENT_ID '
    sql += 'and c.OBS_ENT_ID = b.OBS_ENT_ID '
    sql += 'and d.ENT_SRC_KEY = c.ENT_SRC_KEY and d.DSRC_ID = c.DSRC_ID and d.ETYPE_ID = c.ETYPE_ID '
    sql += 'and a.RES_REL_ID between ? and ? '
    sql += ') a '
    sql += 'order by a.RES_REL_ID '

    statPack['TOTAL_AMBIGUOUS_MATCHES'] = 0
    statPack['TOTAL_POSSIBLE_MATCHES'] = 0
    statPack['TOTAL_POSSIBLY_RELATEDS'] = 0
    statPack['TOTAL_DISCLOSED_RELATIONS'] = 0

    #--start processing rows
    print('Processing relationships ...')
    begResRelId = 1
    endResRelId = chunkSize
    lastResRelId = 0
    relCount = 0
    batchStartTime = time.time()
    while True:
        print('Getting relationships from %s to %s ...' % (begResRelId, endResRelId))
        cursor1 = g2Dbo.sqlExec(sql, (begResRelId, endResRelId, begResRelId, endResRelId))
        rowData = g2Dbo.fetchNext(cursor1)
        if not rowData and lastResRelId >= maxResRelId:
            print(' No more found, processing complete!')
            break
        while rowData:
            relCount += 1

            #--create relation summary
            relData = {}
            relData['RES_REL_ID'] = rowData['RES_REL_ID']
            relData['IS_DISCLOSED'] = rowData['IS_DISCLOSED']
            relData['IS_AMBIGUOUS'] = rowData['IS_AMBIGUOUS']
            relData['MATCH_KEY'] = rowData['MATCH_KEY']
            try: relData['MATCH_LEVEL'] = erruleLookup[rowData['LAST_ERRULE_ID']]['RTYPE_ID']
            except: relData['MATCH_LEVEL'] = 3
            try: relData['ERRULE_CODE'] = erruleLookup[rowData['LAST_ERRULE_ID']]['ERRULE_CODE']
            except: relData['ERRULE_CODE'] = 'unk'
            relData['RECORD_SUMMARY'] = {}
            lastResRelId = relData['RES_REL_ID']
            doesNotMatchFilter = (relationshipFilter == 2 and relData['MATCH_LEVEL'] > 2)

            #--get min and max res_ent_id data
            while rowData and rowData['RES_REL_ID'] == relData['RES_REL_ID']:
                if not doesNotMatchFilter:
                    try: rowData['DATA_SOURCE'] = dsrcLookup[rowData['DSRC_ID']]['DSRC_CODE']
                    except: rowData['DATA_SOURCE'] = 'unk'
                    if rowData['RES_ENT_ID'] not in relData['RECORD_SUMMARY']:
                        relData['RECORD_SUMMARY'][rowData['RES_ENT_ID']] = {}
                    if rowData['DATA_SOURCE'] not in relData['RECORD_SUMMARY'][rowData['RES_ENT_ID']]:
                        relData['RECORD_SUMMARY'][rowData['RES_ENT_ID']][rowData['DATA_SOURCE']] = 1

                    if exportFilePath:
                        columnValues = []
                        columnValues.append(str(rowData['MIN_RES_ENT_ID'] if rowData['MIN_RES_ENT_ID'] != rowData['RES_ENT_ID'] else rowData['MAX_RES_ENT_ID']))
                        columnValues.append(str(rowData['RES_ENT_ID']))
                        columnValues.append(str(relData['MATCH_LEVEL']))
                        columnValues.append(relData['MATCH_KEY'][1:] if relData['MATCH_KEY'] else '')
                        columnValues.append(rowData['DATA_SOURCE'])
                        columnValues.append(rowData['RECORD_ID'])
                        try: exportFileHandle.write(','.join(columnValues) + '\n')        
                        except IOError as err: 
                            print('')
                            print('ERROR: cannot write to %s \n%s' % (exportFilePath, err))
                            print('')
                            shutDown = True
                            break

                rowData = g2Dbo.fetchNext(cursor1)

            #--filters (orphaned relationships (rare) and only want possible matches
            bypass = False
            entityList = list(relData['RECORD_SUMMARY'])
            if len(entityList) != 2 or doesNotMatchFilter:
                #print ('orphan relationship: res_rel_ID=%s' % relData['RES_REL_ID'])  #--error condition, orphaned relationship
                statPack['ORPHAN_RELATIONSHIP_COUNT'] += 1
                bypass = True

             #--lets count it!
            if not bypass:
                entity1 = entityList[0]
                entity2 = entityList[1]
                randomSampleI = random.randint(1,99)                    
                sampleText = '%s %s' % (entity1, entity2)
                #, relData['MATCH_KEY'], relData['ERRULE_CODE'])

                #--update statpack
                if relData['IS_DISCLOSED'] != 0:
                    statPack['TOTAL_DISCLOSED_RELATIONS'] += 1
                    relType = 'DISCLOSED_RELATION'
                elif relData['IS_AMBIGUOUS'] == 1:
                    statPack['TOTAL_AMBIGUOUS_MATCHES'] += 1
                    relType = 'AMBIGUOUS_MATCH'
                elif relData['MATCH_LEVEL'] == 2:
                    statPack['TOTAL_POSSIBLE_MATCHES'] += 1
                    relType = 'POSSIBLE_MATCH'
                else:
                    statPack['TOTAL_POSSIBLY_RELATEDS'] += 1
                    relType = 'POSSIBLY_RELATED'

                #--get data source stats and examples
                for dataSource1 in relData['RECORD_SUMMARY'][entity1]:
                    if dataSource1 not in statPack['DATA_SOURCES']:
                        statPack['DATA_SOURCES'][dataSource1] = {}
                        statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'] = {}
                    for dataSource2 in relData['RECORD_SUMMARY'][entity2]:
                        if dataSource2 not in statPack['DATA_SOURCES']:
                            statPack['DATA_SOURCES'][dataSource2] = {}
                            statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'] = {}

                        if dataSource1 == dataSource2:
                            if relType + '_ENTITY_COUNT' not in statPack['DATA_SOURCES'][dataSource1]: 
                                statPack['DATA_SOURCES'][dataSource1][relType + '_RECORD_COUNT'] = relData['RECORD_SUMMARY'][entity1][dataSource1]
                                statPack['DATA_SOURCES'][dataSource1][relType + '_ENTITY_COUNT'] = 1
                                statPack['DATA_SOURCES'][dataSource1][relType + '_SAMPLE'] = []
                            else:
                                statPack['DATA_SOURCES'][dataSource1][relType + '_RECORD_COUNT'] += relData['RECORD_SUMMARY'][entity1][dataSource1]
                                statPack['DATA_SOURCES'][dataSource1][relType + '_ENTITY_COUNT'] += 1
                            if len(statPack['DATA_SOURCES'][dataSource1][relType + '_SAMPLE']) < sampleSize:
                                statPack['DATA_SOURCES'][dataSource1][relType + '_SAMPLE'].append(sampleText)
                            elif randomSampleI % 10 != 0:
                                statPack['DATA_SOURCES'][dataSource1][relType + '_SAMPLE'][randomSampleI] = sampleText
                        else:
                            #--side1
                            if dataSource2 not in statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES']:
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2] = {}
                            if relType + '_ENTITY_COUNT' not in statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2]: 
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_RECORD_COUNT'] = relData['RECORD_SUMMARY'][entity1][dataSource1]
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_ENTITY_COUNT'] = 1
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_SAMPLE'] = []
                            else:
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_RECORD_COUNT'] += relData['RECORD_SUMMARY'][entity1][dataSource1]
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_ENTITY_COUNT'] += 1
                            if len(statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_SAMPLE']) < sampleSize:
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_SAMPLE'].append(sampleText)
                            elif randomSampleI % 10 != 0:
                                statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][relType + '_SAMPLE'][randomSampleI] = sampleText
                            #--side2
                            if dataSource1 not in statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES']:
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1] = {}
                            if relType + '_ENTITY_COUNT' not in statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1]: 
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_RECORD_COUNT'] = relData['RECORD_SUMMARY'][entity2][dataSource2]
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_ENTITY_COUNT'] = 1
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_SAMPLE'] = []
                            else:
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_RECORD_COUNT'] += relData['RECORD_SUMMARY'][entity2][dataSource2]
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_ENTITY_COUNT'] += 1
                            if len(statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_SAMPLE']) < sampleSize:
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_SAMPLE'].append(sampleText)
                            elif randomSampleI % 10 != 0:
                                statPack['DATA_SOURCES'][dataSource2]['CROSS_MATCHES'][dataSource1][relType + '_SAMPLE'][randomSampleI] = sampleText

            #--status display
            if relCount % progressInterval == 0 or not rowData:
                now = datetime.now().strftime('%I:%M%p').lower()
                elapsedMins = round((time.time() - procStartTime) / 60, 1)
                eps = int(float(progressInterval) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
                batchStartTime = time.time()
                if rowData:
                    print(' %s relationships processed at %s, %s per second' % (relCount, now, eps))
                else:
                    print(' %s relationships completed at %s after %s minutes' % (relCount, now, elapsedMins))

            #--get out if errors hit or out of records
            if shutDown or not rowData:
                break

        #--get out if errors hit
        if shutDown:
            break
        else: #--set next batch
            begResRelId += chunkSize
            endResRelId += chunkSize

    if shutDown:
        return 1

    return 0

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shutDown
    shutDown = True
    return

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    try: response = input(question)
    except: response = None
    return response

#----------------------------------------
if __name__ == '__main__':
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()
    progressInterval = 10000

    #--defaults
    iniFileName = os.getenv('SENZING_CONFIG_FILE') if os.getenv('SENZING_CONFIG_FILE', None) else appPath + os.path.sep + 'G2Module.ini'
    outputFileRoot = os.getenv('SENZING_OUTPUT_FILE_ROOT') if os.getenv('SENZING_INI_FILE_NAME', None) else None
    sampleSize = int(os.getenv('SENZING_SAMPLE_SIZE')) if os.getenv('SENZING_SAMPLE_SIZE', None) and os.getenv('SENZING_SAMPLE_SIZE').isdigit() else 1000
    relationshipFilter = int(os.getenv('SENZING_RELATIONSHIP_FILTER')) if os.getenv('SENZING_RELATIONSHIP_FILTER', None) and os.getenv('SENZING_RELATIONSHIP_FILTER').isdigit() else 3
    chunkSize = int(os.getenv('SENZING_CHUNK_SIZE')) if os.getenv('SENZING_CHUNK_SIZE', None) and os.getenv('SENZING_CHUNK_SIZE').isdigit() else 1000000

    #--capture the command line arguments
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-o', '--output_file_root', dest='output_file_root', default=outputFileRoot, help='root name for files created such as "/project/snapshots/snapshot1"')
    argParser.add_argument('-c', '--ini_file_name', dest='ini_file_name', default=iniFileName, help='name of the g2.ini file, defaults to %s' % iniFileName)
    argParser.add_argument('-s', '--sample_size', dest='sample_size', type=int, default=sampleSize, help='defaults to %s' % sampleSize)
    argParser.add_argument('-f', '--relationship_filter', dest='relationship_filter', type=int, default=relationshipFilter, help='filter options 1=No Relationships, 2=Include possible matches, 3=Include possibly related and disclosed. Defaults to %s' % relationshipFilter)
    argParser.add_argument('-n', '--no_csv_export', dest='no_csv_export', action='store_true', default=False, help='compute json stats only, do not export csv file')
    argParser.add_argument('-k', '--chunk_size', dest='chunk_size', type=int, default=chunkSize, help='chunk size: number of records to query at a time, defaults to %s' % chunkSize)
    args = argParser.parse_args()
    iniFileName = args.ini_file_name
    outputFileRoot = args.output_file_root
    sampleSize = args.sample_size
    relationshipFilter = args.relationship_filter
    noCsvExport = args.no_csv_export
    chunkSize = args.chunk_size

    #--get parameters from ini file
    if not os.path.exists(iniFileName):
        print('')
        print('An ini file was not found, please supply with the -c parameter.')
        print('')
        sys.exit(1)
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: g2dbUri = iniParser.get('SQL', 'CONNECTION')
    except: 
        print('')
        print('CONNECTION parameter not found in [SQL] section of the ini file')
        print('')
        sys.exit(1)

    #--try to open the database
    g2Dbo = G2Database(g2dbUri)
    if not g2Dbo.success:
        print('')
        print('Could not connect to database')
        print('')
        sys.exit(1)

    #--use config file if in the ini file, otherwise expect to get from database with config manager lib
    try: configTableFile = iniParser.get('SQL', 'G2CONFIGFILE')
    except: configTableFile = None
    if not configTableFile and not G2ConfigMgr:
        print('')
        print('Config information missing from ini file and no config manager present!')
        print('')
        sys.exit(1)

    #--get the config from the file
    if configTableFile:
        try: cfgData = json.load(open(configTableFile), encoding="utf-8")
        except ValueError as e:
            print('')
            print('G2CONFIGFILE: %s has invalid json' % configTableFile)
            print(e)
            print('')
            sys.exit(1)
        except IOError as e:
            print('')
            print('G2CONFIGFILE: %s was not found' % configTableFile)
            print(e)
            print('')
            sys.exit(1)

    #--get the config from the config manager
    else:
        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(iniFileName)
        try: 
            g2ConfigMgr = G2ConfigMgr()
            g2ConfigMgr.initV2('pyG2ConfigMgr', iniParams, False)
            defaultConfigID = bytearray() 
            g2ConfigMgr.getDefaultConfigID(defaultConfigID)
            if len(defaultConfigID) == 0:
                print('')
                print('No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            defaultConfigDoc = bytearray() 
            g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
            if len(defaultConfigDoc) == 0:
                print('')
                print('No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            cfgData = json.loads(defaultConfigDoc.decode())
            g2ConfigMgr.destroy()
        except:
            #--error already printed by the api wrapper
            sys.exit(1)

    #--need these config tables in memory for fast lookup
    dsrcLookup = {}
    for cfgRecord in cfgData['G2_CONFIG']['CFG_DSRC']:
        dsrcLookup[cfgRecord['DSRC_ID']] = cfgRecord 
    erruleLookup = {}
    for cfgRecord in cfgData['G2_CONFIG']['CFG_ERRULE']:
        erruleLookup[cfgRecord['ERRULE_ID']] = cfgRecord 
    ambiguousFtypeID = 0
    ftypeLookup = {}
    for cfgRecord in cfgData['G2_CONFIG']['CFG_FTYPE']:
        ftypeLookup[cfgRecord['FTYPE_ID']] = cfgRecord 
        if cfgRecord['FTYPE_CODE'] == 'AMBIGUOUS_ENTITY':
            ambiguousFtypeID = cfgRecord['FTYPE_ID']

    #--check the output file
    if not outputFileRoot:
        print('')
        print('Please use -o to select and output path and root file name such as /project/audit/run1')
        print('')
        sys.exit(1)
    if os.path.splitext(outputFileRoot)[1]:
        print('')
        print("Please don't use a file extension as both a .json and a .csv file will be created")
        print('')
        sys.exit(1)

    #--create output file paths
    statsFilePath = outputFileRoot + '.json'
    if noCsvExport:
        exportFilePath = None
    else:
        exportFilePath = outputFileRoot + '.csv'

    #--open the export file if set
    if exportFilePath:
        columnHeaders = []
        columnHeaders.append('RESOLVED_ENTITY_ID')
        columnHeaders.append('RELATED_ENTITY_ID')
        columnHeaders.append('MATCH_LEVEL')
        columnHeaders.append('MATCH_KEY')
        columnHeaders.append('DATA_SOURCE')
        columnHeaders.append('RECORD_ID')
        try: 
            exportFileHandle = open(exportFilePath, 'w')
            exportFileHandle.write(','.join(columnHeaders) + '\n')        
        except IOError as err: 
            print('')
            print('ERROR: cannot write to %s \n%s' % (exportFilePath, err))
            print('')
            sys.exit(1)
            
    #--get entities and relationships
    statPack = {}
    statPack['SOURCE'] = 'pocSnapshot'
    statPack['VERSION'] = pocUtilsVersion
    statPack['ORPHAN_RELATIONSHIP_COUNT'] = 0
    returnCode = processEntities()
    if returnCode == 0 and relationshipFilter in (2,3):
        returnCode = processRelationships()

    #--wrap ups
    if exportFilePath:
        exportFileHandle.close()

    #--dump the stats to screen and file
    print('')
    for stat in statPack:
        if type(statPack[stat]) not in (list, dict):
            print ('%s = %s' % (stat, statPack[stat]))
    with open(statsFilePath, 'w') as outfile:
        json.dump(statPack, outfile)    
    print('')

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if returnCode == 0:
        print('Process completed successfully in %s minutes' % elapsedMins)
    else:
        print('Process aborted after %s minutes!' % elapsedMins)
    print('')

    g2Dbo.close()

    sys.exit(returnCode)
