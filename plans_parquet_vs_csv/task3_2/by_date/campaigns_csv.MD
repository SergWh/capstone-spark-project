== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['totalCost DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'sum('billingCost) AS totalCost#2697]
         +- 'Filter ('isConfirmed = true)
            +- 'UnresolvedRelation [campaigns], [], false

== Analyzed Logical Plan ==
campaignId: string, totalCost: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [totalCost#2697 DESC NULLS LAST], true
      +- Aggregate [campaignId#83], [campaignId#83, sum(cast(billingCost#44 as double)) AS totalCost#2697]
         +- Filter (isConfirmed#45 = true)
            +- SubqueryAlias campaigns
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
GlobalLimit 10
+- LocalLimit 10
   +- Sort [totalCost#2697 DESC NULLS LAST], true
      +- Aggregate [campaignId#83], [campaignId#83, sum(cast(billingCost#44 as double)) AS totalCost#2697]
         +- Project [campaignId#83, billingCost#44]
            +- Join Inner, (purchaseId#59 = purchaseId#42)
               :- Project [purchaseId#59, campaignId#83]
               :  +- Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(purchaseId#59))
               :     +- Window [first(campaignId#58, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS campaignId#83], [userId#16]
               :        +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#59, userId#16, element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), campaign_id, false) AS campaignId#58, eventType#18]
               :           +- Relation[userId#16,eventId#17,eventType#18,eventTime#19,attributes#20] csv
               +- Project [purchaseId#42, billingCost#44]
                  +- Filter ((((isnotnull(purchaseTime#43) AND isnotnull(isConfirmed#45)) AND (cast(purchaseTime#43 as date) = 18577)) AND (isConfirmed#45 = true)) AND isnotnull(purchaseId#42))
                     +- Relation[purchaseId#42,purchaseTime#43,billingCost#44,isConfirmed#45] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[totalCost#2697 DESC NULLS LAST], output=[campaignId#83,totalCost#2697])
+- *(7) HashAggregate(keys=[campaignId#83], functions=[sum(cast(billingCost#44 as double))], output=[campaignId#83, totalCost#2697])
   +- Exchange hashpartitioning(campaignId#83, 200), ENSURE_REQUIREMENTS, [id=#667]
      +- *(6) HashAggregate(keys=[campaignId#83], functions=[partial_sum(cast(billingCost#44 as double))], output=[campaignId#83, sum#2702])
         +- *(6) Project [campaignId#83, billingCost#44]
            +- *(6) SortMergeJoin [purchaseId#59], [purchaseId#42], Inner
               :- *(3) Sort [purchaseId#59 ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(purchaseId#59, 200), ENSURE_REQUIREMENTS, [id=#649]
               :     +- *(2) Project [purchaseId#59, campaignId#83]
               :        +- *(2) Filter ((isnotnull(eventType#18) AND (eventType#18 = purchase)) AND isnotnull(purchaseId#59))
               :           +- Window [first(campaignId#58, true) windowspecdefinition(userId#16, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS campaignId#83], [userId#16]
               :              +- *(1) Sort [userId#16 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#16, 200), ENSURE_REQUIREMENTS, [id=#623]
               :                    +- Project [element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), purchase_id, false) AS purchaseId#59, userId#16, element_at(from_json(MapType(StringType,StringType,true), attributes#20, Some(Europe/Moscow)), campaign_id, false) AS campaignId#58, eventType#18]
               :                       +- FileScan csv [userId#16,eventType#18,attributes#20] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/bigdata-input-genera..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<userId:string,eventType:string,attributes:string>
               +- *(5) Sort [purchaseId#42 ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(purchaseId#42, 200), ENSURE_REQUIREMENTS, [id=#658]
                     +- *(4) Project [purchaseId#42, billingCost#44]
                        +- *(4) Filter ((((isnotnull(purchaseTime#43) AND isnotnull(isConfirmed#45)) AND (cast(purchaseTime#43 as date) = 18577)) AND (isConfirmed#45 = true)) AND isnotnull(purchaseId#42))
                           +- FileScan csv [purchaseId#42,purchaseTime#43,billingCost#44,isConfirmed#45] Batched: false, DataFilters: [isnotnull(purchaseTime#43), isnotnull(isConfirmed#45), (cast(purchaseTime#43 as date) = 18577), ..., Format: CSV, Location: InMemoryFileIndex[file:/Users/schernonog/IdeaProjects/capstone-spark-project/bigdata-input-genera..., PartitionFilters: [], PushedFilters: [IsNotNull(purchaseTime), IsNotNull(isConfirmed), EqualTo(isConfirmed,true), IsNotNull(purchaseId)], ReadSchema: struct<purchaseId:string,purchaseTime:string,billingCost:string,isConfirmed:boolean>
