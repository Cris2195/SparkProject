class Forkee(archive_url:String,
             archived :Boolean,
             assignees_url:String,
             blobs_url:String,
             branches_url:String,
             clone_url:String,
             collaborators_url:String,
             comments_url:String,
             commits_url:String,
             compare_url:String,
             contents_url:String,
             contributors_url:String,
             created_at:String,
             default_branch:String,
             deployments_url:String,
             description:String,
             downloads_url:String,
             events_url:String,
             fork:Boolean,
             forks:Long,
             forks_count:Long,
             forks_url:String,
             full_name:String,
             git_commits_url:String,
             git_refs_url:String,
             git_tags_url:String,
             git_url:String,
             has_downloads:Boolean,
             has_issues:Boolean,
             has_pages:Boolean,
             has_projects:Boolean,
             has_wiki:Boolean,
             homepage:String,
             hooks_url:String,
             html_url:String,
             id:Long,
             issue_comment_url:String,
             issue_events_url:String,
             issues_url:String,
             keys_url:String,
             labels_url:String,
             language:String,
             languages_url:String,
             license : License
            ) extends Product with Serializable{
  def canEqual(that: Any) = that.isInstanceOf[Forkee]

  def productArity = 43 // number of columns

  def productElement(idx: Int) = idx match {
    case 0 =>archive_url
    case 1 => archived
    case 2 => assignees_url
    case 3 =>blobs_url
    case 4 => branches_url
    case 5 => clone_url
    case 6 =>collaborators_url
    case 7 =>comments_url
    case 8 => commits_url
    case 9 =>compare_url
    case 10 =>contents_url
    case 11 => contributors_url
    case 12 =>created_at
    case 13 =>default_branch
    case 14 => deployments_url
    case 15 =>description
    case 16 => downloads_url
    case 17 =>events_url
    case 18 => fork
    case 19 =>forks
    case 20 =>forks_count
    case 21 => forks_url
    case 22 =>full_name
    case 23 =>git_commits_url
    case 24 =>git_refs_url
    case 25 =>git_tags_url
    case 26 =>git_url
    case 27 => has_downloads
    case 28 => has_issues
    case 29 => has_pages
    case 30 =>has_projects
    case 31 =>has_wiki
    case 32 => homepage
    case 33 =>hooks_url
    case 34 =>html_url
    case 35 =>id
    case 36 => issue_comment_url
    case 37 => issue_events_url
    case 38 => issues_url
    case 39 =>keys_url
    case 40 =>labels_url
    case 41 => language
    case 42 =>languages_url
    case 43 =>license
  }
}
