== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['totalCost DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'sum('billingCost) AS totalCost#2782]
         +- 'Filter ('isConfirmed = true)
            +- 'UnresolvedRelation [campaigns], [], false

== Analyzed Logical Plan ==
campaignId: string, totalCost: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [totalCost#2782 DESC NULLS LAST], true
      +- Aggregate [campaignId#2659], [campaignId#2659, sum(cast(billingCost#2623 as double)) AS totalCost#2782]
         +- Filter (isConfirmed#2624 = true)
            +- SubqueryAlias campaigns
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
GlobalLimit 10
+- LocalLimit 10
   +- Sort [totalCost#2782 DESC NULLS LAST], true
      +- Aggregate [campaignId#2659], [campaignId#2659, sum(cast(billingCost#2623 as double)) AS totalCost#2782]
         +- Project [campaignId#2659, billingCost#2623]
            +- Join Inner, (purchaseId#2635 = purchaseId#2621)
               :- Project [purchaseId#2635, campaignId#2659]
               :  +- Filter ((isnotnull(eventType#2613) AND (eventType#2613 = purchase)) AND isnotnull(purchaseId#2635))
               :     +- Window [first(campaignId#2634, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS campaignId#2659], [userId#2611]
               :        +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#2635, userId#2611, element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), campaign_id, false) AS campaignId#2634, eventType#2613]
               :           +- Relation[userId#2611,eventId#2612,eventType#2613,eventTime#2614,attributes#2615] parquet
               +- Project [purchaseId#2621, billingCost#2623]
                  +- Filter ((((((isnotnull(month#2626) AND isnotnull(year#2625)) AND isnotnull(isConfirmed#2624)) AND (9 = month#2626)) AND (2020 = year#2625)) AND (isConfirmed#2624 = true)) AND isnotnull(purchaseId#2621))
                     +- Relation[purchaseId#2621,purchaseTime#2622,billingCost#2623,isConfirmed#2624,year#2625,month#2626] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[totalCost#2782 DESC NULLS LAST], output=[campaignId#2659,totalCost#2782])
+- *(8) HashAggregate(keys=[campaignId#2659], functions=[sum(cast(billingCost#2623 as double))], output=[campaignId#2659, totalCost#2782])
   +- Exchange hashpartitioning(campaignId#2659, 200), ENSURE_REQUIREMENTS, [id=#1292]
      +- *(7) HashAggregate(keys=[campaignId#2659], functions=[partial_sum(cast(billingCost#2623 as double))], output=[campaignId#2659, sum#2787])
         +- *(7) Project [campaignId#2659, billingCost#2623]
            +- *(7) SortMergeJoin [purchaseId#2635], [purchaseId#2621], Inner
               :- *(4) Sort [purchaseId#2635 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(purchaseId#2635, 200), ENSURE_REQUIREMENTS, [id=#1273]
               :     +- *(3) Project [purchaseId#2635, campaignId#2659]
               :        +- *(3) Filter ((isnotnull(eventType#2613) AND (eventType#2613 = purchase)) AND isnotnull(purchaseId#2635))
               :           +- Window [first(campaignId#2634, true) windowspecdefinition(userId#2611, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS campaignId#2659], [userId#2611]
               :              +- *(2) Sort [userId#2611 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#2611, 200), ENSURE_REQUIREMENTS, [id=#1264]
               :                    +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#2635, userId#2611, element_at(from_json(MapType(StringType,StringType,true), attributes#2615, Some(Europe/Moscow)), campaign_id, false) AS campaignId#2634, eventType#2613]
               :                       +- *(1) ColumnarToRow
               :                          +- FileScan parquet [userId#2611,eventType#2613,attributes#2615] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/parquet-dataset/clic..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,attributes:string>
               +- *(6) Sort [purchaseId#2621 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(purchaseId#2621, 200), ENSURE_REQUIREMENTS, [id=#1283]
                     +- *(5) Project [purchaseId#2621, billingCost#2623]
                        +- *(5) Filter ((isnotnull(isConfirmed#2624) AND (isConfirmed#2624 = true)) AND isnotnull(purchaseId#2621))
                           +- *(5) ColumnarToRow
                              +- FileScan parquet [purchaseId#2621,billingCost#2623,isConfirmed#2624,year#2625,month#2626] Batched: true, DataFilters: [isnotnull(isConfirmed#2624), (isConfirmed#2624 = true), isnotnull(purchaseId#2621)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/parquet-dataset/user..., PartitionFilters: [isnotnull(month#2626), isnotnull(year#2625), (9 = month#2626), (2020 = year#2625)], PushedFilters: [IsNotNull(isConfirmed), EqualTo(isConfirmed,true), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,billingCost:string,isConfirmed:boolean>
