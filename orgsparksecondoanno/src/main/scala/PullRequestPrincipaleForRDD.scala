class PullRequestPrincipaleForRDD(
                             val _links:_Links,
                             val additions:BigInt,
                             val assignee: AssigneeForRDD,
                             val assignees: Array [AssigneeForRDD],
                             val author_association:String,
                             val base: BaseForRDD,
                             val body:String,
                             val changed_files:BigInt,
                             val closed_at:String,
                             val comments:BigInt,
                             val comments_url:String,
                             val commits:BigInt,
                             val commits_url:String,
                             val created_at:String,
                             val deletions:BigInt,
                             val diff_url:String,
                             val head: HeadForRDD,
                             val html_url:String,
                             val id:BigInt,
                             val issue_url:String,
                             val labels: Array[LabelsForRDD],
                             val locked:java.lang.Boolean,
                             val maintainer_can_modify:java.lang.Boolean,
                             val merge_commit_sha:String,
                             val mergeable:java.lang.Boolean,
                             val mergeable_state:String,
                             val merged:java.lang.Boolean,
                             val merged_at:String,
                             val merged_by: MergedByForRDD,
                             val milestone: MilestoneForRDD,
                             val number:BigInt,
                             val patch_url:String,
                             val rebaseable:java.lang.Boolean,
                             val requested_reviewers: Array[RequestedReviewersForRDD],
                             val requested_teams:Array[RequestedTeams],
                             val review_comment_url:String,
                             val review_comments:BigInt,
                             val review_comments_url:String,
                             val state:String,
                             val statuses_url:String,
                             val title:String,
                             val updated_at:String,
                             val url:String,
                             val user: UserForRDD


                           )extends Product with Serializable {
                            def canEqual(that: Any) = that.isInstanceOf[PullRequestPrincipaleForRDD]
                            def productArity = 71 // number of columns

                            def productElement(idx: Int) = idx match {

                              case 0 => _links
                              case 1 => additions
                              case 2 => assignee
                              case 3 => assignees
                              case 4 => author_association
                              case 5 => base
                              case 6 => body
                              case 7 => changed_files
                              case 8 => closed_at
                              case 9 => comments
                              case 10=> comments_url
                              case 11=> commits
                              case 12=> commits_url
                              case 13=> created_at
                              case 14=> deletions
                              case 15=> diff_url
                              case 16=> head
                              case 17=> html_url
                              case 18=> id
                              case 19=> issue_url
                              case 20=> labels
                              case 21=> locked
                              case 22=> maintainer_can_modify
                              case 23=> merge_commit_sha
                              case 24=> mergeable
                              case 25=> mergeable_state
                              case 26=> merged
                              case 27=> merged_at
                              case 28=> merged_by
                              case 29=> milestone
                              case 30=> number
                              case 31=> patch_url
                              case 32=> rebaseable
                              case 33=> requested_reviewers
                              case 34=> requested_teams
                              case 35=> review_comment_url
                              case 36=> review_comments
                              case 37=> review_comments_url
                              case 38=> state
                              case 39=> statuses_url
                              case 40=> title
                              case 41=> updated_at
                              case 42=> url
                              case 43=> user
                            }}
