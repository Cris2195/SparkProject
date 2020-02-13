class IssuePrincipale(
                         val assignee: Assignee,
                         val assignees:Array [Assignee],
                         val author_association:String,
                         val body:String,
                         val closed_at:String,
                         val comments:BigInt,
                         val comments_url:String,
                         val created_at:String,
                         val events_url:String,
                         val html_url:String,
                         val id:BigInt,
                         val labels: Array [Labels],
                         val labels_url:String,
                         val locked:java.lang.Boolean,
                         val milestone: Milestone,
                         val number:BigInt,
                         val pullRequest: PullRequest,
                         val repository_url:String,
                         val state:String,
                         val title:String,
                         val updated_at:String,
                         val url:String,
                         val user: User
                          )extends  Product with Serializable {
                            def canEqual(that: Any) = that.isInstanceOf[IssuePrincipale]
                            def productArity = 23 // number of columns

                            def productElement(idx: Int) = idx match {
                              case 0 => assignee
                              case 1 => assignees
                              case 2 => author_association
                              case 3 => body
                              case 4 => closed_at
                              case 5 => comments
                              case 6 => comments_url
                              case 7 => created_at
                              case 8 => events_url
                              case 9 => html_url
                              case 10=> id
                              case 11=> labels
                              case 12=> labels_url
                              case 13=> locked
                              case 14=> milestone
                              case 15=> number
                              case 16=> pullRequest
                              case 17=> repository_url
                              case 18=> state
                              case 19=> title
                              case 20=> updated_at
                              case 21=> url
                              case 22=> user
                            }
}

