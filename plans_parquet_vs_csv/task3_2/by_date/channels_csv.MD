== Parsed Logical Plan ==
'Sort ['sessionsCount DESC NULLS LAST], true
+- 'Aggregate ['channelId], ['channelId, 'count('channelId) AS sessionsCount#2710]
   +- 'UnresolvedRelation [channels], [], false

== Analyzed Logical Plan ==
channelId: string, sessionsCount: bigint
Sort [sessionsCount#2710L DESC NULLS LAST], true
+- Aggregate [channelId#81], [channelId#81, count(channelId#81) AS sessionsCount#2710L]
   +- SubqueryAlias channels
      +- Filter (to_date(purchaseTime#43, None) = to_date(1605078610000000, None))
         +- Project [purchaseId#59, channelId#81, campaignId#83, sessionId#89, purchaseTime#43, billingCost#44, isConfirmed#45]
            +- Join Inner, (purchaseId#59 = purchaseId#42)
               :- Project [purchaseId#59, channelId#81, campaignId#83, sessionId#89]
               :  +- Project [purchaseId#59, sessionEnd#79, channelId#81, campaignId#83, sessionId#89]
               :     +- Project [purchaseId#59, sessionStart#77, sessionEnd#79, channelId#81, campaignId#83, concat(sessionStart#77, :, sessionEnd#79) AS sessionId#89]
               :        +- Project [purchaseId#59, sessionStart#77, sessionEnd#79, channelId#81, campaignId#83]
               :           +- Filter (eventType#18 = purchase)
               :              +- Project [purchaseId#59, sessionStart#77, sessionEnd#79, channelId#81, campaignId#83, eventType#18]
               :                 +- Project [purchaseId#59, sessionStart#60, userId#16, sessionEnd#61, channelId#57, campaignId#58, sessionStart#77, sessionEnd#79, channelId#81, campaignId#83, sessionStart#77, sessionEnd#79, channelId#81, campaignId#83, eventType#18]
               :                    +- Window [first(sessionStart#60, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sessionStart#77, first(sessionEnd#61, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sessionEnd#79, first(channelId#57, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS channelId#81, first(campaignId#58, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS campaignId#83], [userId#16]
               :                       +- Project [purchaseId#59, sessionStart#60, userId#16, sessionEnd#61, channelId#57, campaignId#58, eventType#18]
               :                          +- Project [userId#16, eventType#18, element_at(attrs#62, purchase_id, false) AS purchaseId#59, element_at(attrs#62, channel_id, false) AS channelId#57, element_at(attrs#62, campaign_id, false) AS campaignId#58, CASE WHEN (eventType#18 = app_open) THEN eventId#17 ELSE cast(null as string) END AS sessionStart#60, CASE WHEN (eventType#18 = app_close) THEN eventId#17 ELSE cast(null as string) END AS sessionEnd#61]
               :                             +- Project [userId#16, eventId#17, eventType#18, eventTime#19, attributes#20, from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)) AS attrs#62]
               :                                +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
               +- Relation[purchaseId#42,purchaseTime#43,billingCost#44,isConfirmed#45] csv

== Optimized Logical Plan ==
Sort [sessionsCount#2710L DESC NULLS LAST], true
+- Aggregate [channelId#81], [channelId#81, count(channelId#81) AS sessionsCount#2710L]
   +- Project [channelId#81]
      +- Join Inner, (purchaseId#59 = purchaseId#42)
         :- Project [purchaseId#59, channelId#81]
         :  +- Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(purchaseId#59))
         :     +- Window [first(channelId#57, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS channelId#81], [userId#16]
         :        +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#59, userId#16, element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), channel_id, false) AS channelId#57, eventType#18]
         :           +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
         +- Project [purchaseId#42]
            +- Filter ((isnotnull(purchaseTime#43) AND (cast(purchaseTime#43 as date) = 18577)) AND isnotnull(purchaseId#42))
               +- Relation[purchaseId#42,purchaseTime#43,billingCost#44,isConfirmed#45] csv

== Physical Plan ==
*(5) Sort [sessionsCount#2710L DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(sessionsCount#2710L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [id=#756]
   +- *(4) HashAggregate(keys=[channelId#81], functions=[count(channelId#81)], output=[channelId#81, sessionsCount#2710L])
      +- Exchange hashpartitioning(channelId#81, 200), ENSURE_REQUIREMENTS, [id=#752]
         +- *(3) HashAggregate(keys=[channelId#81], functions=[partial_count(channelId#81)], output=[channelId#81, count#2716L])
            +- *(3) Project [channelId#81]
               +- *(3) BroadcastHashJoin [purchaseId#59], [purchaseId#42], Inner, BuildRight, false
                  :- *(3) Project [purchaseId#59, channelId#81]
                  :  +- *(3) Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(purchaseId#59))
                  :     +- Window [first(channelId#57, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS channelId#81], [userId#16]
                  :        +- *(1) Sort [userId#16 ASC NULLS FIRST], false, 0
                  :           +- Exchange hashpartitioning(userId#16, 200), ENSURE_REQUIREMENTS, [id=#719]
                  :              +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#59, userId#16, element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), channel_id, false) AS channelId#57, eventType#18]
                  :                 +- FileScan csv [userId#16,eventType#18,attributes#20] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/bigdata-input-genera..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,attributes:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [id=#746]
                     +- *(2) Project [purchaseId#42]
                        +- *(2) Filter ((isnotnull(purchaseTime#43) AND (cast(purchaseTime#43 as date) = 18577)) AND isnotnull(purchaseId#42))
                           +- FileScan csv [purchaseId#42,purchaseTime#43] Batched: false, DataFilters: [isnotnull(purchaseTime#43), (cast(purchaseTime#43 as date) = 18577), isnotnull(purchaseId#42)], Format: CSV, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/bigdata-input-genera..., PartitionFilters: [], PushedFilters: [IsNotNull(purchaseTime), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,purchaseTime:string>
