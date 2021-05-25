== Parsed Logical Plan ==
'Sort ['sessionsCount DESC NULLS LAST], true
+- 'Aggregate ['channelId], ['channelId, 'count('channelId) AS sessionsCount#2795]
   +- 'UnresolvedRelation [channels], [], false

== Analyzed Logical Plan ==
channelId: string, sessionsCount: bigint
Sort [sessionsCount#2795L DESC NULLS LAST], true
+- Aggregate [channelId#2657], [channelId#2657, count(channelId#2657) AS sessionsCount#2795L]
   +- SubqueryAlias channels
      +- Filter ((month(cast(1598944210000000 as date)) = month#2626) AND (year(cast(1598944210000000 as date)) = year#2625))
         +- Project [purchaseId#2635, channelId#2657, campaignId#2659, sessionId#2665, purchaseTime#2622, billingCost#2623, isConfirmed#2624, year#2625, month#2626]
            +- Join Inner, (purchaseId#2635 = purchaseId#2621)
               :- Project [purchaseId#2635, channelId#2657, campaignId#2659, sessionId#2665]
               :  +- Project [purchaseId#2635, sessionEnd#2655, channelId#2657, campaignId#2659, sessionId#2665]
               :     +- Project [purchaseId#2635, sessionStart#2653, sessionEnd#2655, channelId#2657, campaignId#2659, concat(sessionStart#2653, :, sessionEnd#2655) AS sessionId#2665]
               :        +- Project [purchaseId#2635, sessionStart#2653, sessionEnd#2655, channelId#2657, campaignId#2659]
               :           +- Filter (eventType#2613 = purchase)
               :              +- Project [purchaseId#2635, sessionStart#2653, sessionEnd#2655, channelId#2657, campaignId#2659, eventType#2613]
               :                 +- Project [purchaseId#2635, sessionStart#2636, userId#2611, sessionEnd#2637, channelId#2633, campaignId#2634, sessionStart#2653, sessionEnd#2655, channelId#2657, campaignId#2659, sessionStart#2653, sessionEnd#2655, channelId#2657, campaignId#2659, eventType#2613]
               :                    +- Window [first(sessionStart#2636, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sessionStart#2653, first(sessionEnd#2637, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sessionEnd#2655, first(channelId#2633, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS channelId#2657, first(campaignId#2634, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS campaignId#2659], [userId#2611]
               :                       +- Project [purchaseId#2635, sessionStart#2636, userId#2611, sessionEnd#2637, channelId#2633, campaignId#2634, eventType#2613]
               :                          +- Project [userId#2611, eventType#2613, element_at(attrs#2638, purchase_id, false) AS purchaseId#2635, element_at(attrs#2638, channel_id, false) AS channelId#2633, element_at(attrs#2638, campaign_id, false) AS campaignId#2634, CASE WHEN (eventType#2613 = app_open) THEN eventId#2612 ELSE cast(null as string) END AS sessionStart#2636, CASE WHEN (eventType#2613 = app_close) THEN eventId#2612 ELSE cast(null as string) END AS sessionEnd#2637]
               :                             +- Project [userId#2611, eventId#2612, eventType#2613, eventTime#2614, attributes#2615, from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)) AS attrs#2638]
               :                                +- Relation[userId#2611,eventId#2612,eventType#2613,eventTime#2614,attributes#2615] parquet
               +- Relation[purchaseId#2621,purchaseTime#2622,billingCost#2623,isConfirmed#2624,year#2625,month#2626] parquet

== Optimized Logical Plan ==
Sort [sessionsCount#2795L DESC NULLS LAST], true
+- Aggregate [channelId#2657], [channelId#2657, count(channelId#2657) AS sessionsCount#2795L]
   +- Project [channelId#2657]
      +- Join Inner, (purchaseId#2635 = purchaseId#2621)
         :- Project [purchaseId#2635, channelId#2657]
         :  +- Filter ((isnotnull(eventType#2613) AND (eventType#2613 = purchase)) AND isnotnull(purchaseId#2635))
         :     +- Window [first(channelId#2633, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS channelId#2657], [userId#2611]
         :        +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#2635, userId#2611, element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), channel_id, false) AS channelId#2633, eventType#2613]
         :           +- Relation[userId#2611,eventId#2612,eventType#2613,eventTime#2614,attributes#2615] parquet
         +- Project [purchaseId#2621]
            +- Filter ((((isnotnull(month#2626) AND isnotnull(year#2625)) AND (9 = month#2626)) AND (2020 = year#2625)) AND isnotnull(purchaseId#2621))
               +- Relation[purchaseId#2621,purchaseTime#2622,billingCost#2623,isConfirmed#2624,year#2625,month#2626] parquet

== Physical Plan ==
*(6) Sort [sessionsCount#2795L DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(sessionsCount#2795L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [id=#1405]
   +- *(5) HashAggregate(keys=[channelId#2657], functions=[count(channelId#2657)], output=[channelId#2657, sessionsCount#2795L])
      +- Exchange hashpartitioning(channelId#2657, 200), ENSURE_REQUIREMENTS, [id=#1401]
         +- *(4) HashAggregate(keys=[channelId#2657], functions=[partial_count(channelId#2657)], output=[channelId#2657, count#2801L])
            +- *(4) Project [channelId#2657]
               +- *(4) BroadcastHashJoin [purchaseId#2635], [purchaseId#2621], Inner, BuildRight, false
                  :- *(4) Project [purchaseId#2635, channelId#2657]
                  :  +- *(4) Filter ((isnotnull(eventType#2613) AND (eventType#2613 = purchase)) AND isnotnull(purchaseId#2635))
                  :     +- Window [first(channelId#2633, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS channelId#2657], [userId#2611]
                  :        +- *(2) Sort [userId#2611 ASC NULLS FIRST], false, 0
                  :           +- Exchange hashpartitioning(userId#2611, 200), ENSURE_REQUIREMENTS, [id=#1382]
                  :              +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#2635, userId#2611, element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), channel_id, false) AS channelId#2633, eventType#2613]
                  :                 +- *(1) ColumnarToRow
                  :                    +- FileScan parquet [userId#2611,eventType#2613,attributes#2615] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/parquet-dataset/clic..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,attributes:string>
                  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]),false), [id=#1395]
                     +- *(3) Project [purchaseId#2621]
                        +- *(3) Filter isnotnull(purchaseId#2621)
                           +- *(3) ColumnarToRow
                              +- FileScan parquet [purchaseId#2621,year#2625,month#2626] Batched: true, DataFilters: [isnotnull(purchaseId#2621)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/parquet-dataset/user..., PartitionFilters: [isnotnull(month#2626), isnotnull(year#2625), (9 = month#2626), (2020 = year#2625)], PushedFilters: [IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string>
