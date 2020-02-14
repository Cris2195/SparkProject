package com.progetto

class Repo(
         val archive_url : String,
         val archived : java.lang.Boolean,
         val assignees_url : String,
         val blobs_url : String,
         val branches_url : String,
         val clone_url : String,
         val collaborators_url : String,
         val comments_url : String,
         val commits_url : String,
         val compare_url : String,
         val contents_url : String,
         val contributors_url : String,
         val created_at : String,
         val default_branch : String,
         val deployments_url : String,
         val description : String,
         val downloads_url : String,
         val events_url : String,
         val fork : java.lang.Boolean,
         val forks : BigInt,
         val forks_count : BigInt,
         val forks_url : String,
         val full_name : String,
         val git_commits : String,
         val git_refs_url : String,
         val git_tags_url : String,
         val git_url  : String,
         val has_downloads : java.lang.Boolean,
         val has_issues : java.lang.Boolean,
         val has_pages : java.lang.Boolean,
         val has_projects : java.lang.Boolean,
         val has_wiki : java.lang.Boolean,
         val homepage : String,
         val hooks_url : String,
         val html_url : String,
         val id : BigInt,
         val issue_comment_url : String,
         val issue_events_url : String,
         val issues_url : String,
         val keys_url : String,
         val labels_url : String,
         val language : String,
         val languages_url : String,
         val license: License,
         val merges_url:String,
         val milestones_url:String,
         val mirror_url:String,
         val name:String,
         val notifications_url:String,
         val open_issues:BigInt,
         val open_issues_count:BigInt,
         val owner: Owner,
         val pulls_url:String,
         val pushed_at :String,
         val releases_url:String,
         val size:BigInt,
         val ssh_url:String,
         val stargazers_count:BigInt,
         val stargazers_url:String,
         val statuses_url:String,
         val subscribers_url:String,
         val subscription_url:String,
         val  svn_url:String,
         val  tags_url:String,
         val  teams_url:String,
         val  trees_url:String,
         val  updated_at:String,
         val  url:String,
         val  watchers:BigInt,
         val  watchers_count:BigInt
          ) extends Product with Serializable {
           def canEqual(that: Any) = that.isInstanceOf[Repo]
           def productArity = 71 // number of columns

      def productElement(idx: Int) = idx match {

case 0 => archive_url
case 1 => archived
case 2 => assignees_url
case 3 => blobs_url
case 4 => branches_url
case 5 => clone_url
case 6 => collaborators_url
case 7 => comments_url
case 8 => commits_url
case 9 => compare_url
case 10=> contents_url
case 11=> contributors_url
case 12=> created_at
case 13=> default_branch
case 14=> deployments_url
case 15=> description
case 16=> downloads_url
case 17=> events_url
case 18=> fork
case 19=> forks
case 20=> forks_count
case 21=> forks_url
case 22=> full_name
case 23=> git_commits
case 24=> git_refs_url
case 25=> git_tags_url
case 26=> git_url
case 27=> has_downloads
case 28=> has_issues
case 29=> has_pages
case 30=> has_projects
case 31=> has_wiki
case 32=> homepage
case 33=> hooks_url
case 34=> html_url
case 35=> id
case 36=> issue_comment_url
case 37=> issue_events_url
case 38=> issues_url
case 39=> keys_url
case 40=> labels_url
case 41=> language
case 42=> languages_url
case 43=> license
case 44=> merges_url
case 45=> milestones_url
case 46=> mirror_url
case 47=> name
case 48=> notifications_url
case 49=> open_issues
case 50=> open_issues_count
case 51=> owner
case 52=> pulls_url
case 53=> pushed_at
case 54=> releases_url
case 55=> size
case 56=> ssh_url
case 57=> stargazers_count
case 58=> stargazers_url
case 59=> statuses_url
case 60=> subscribers_url
case 61=> subscription_url
case 62=> svn_url
case 63=> tags_url
case 64=> teams_url
case 65=> trees_url
case 66=> updated_at
case 67=> url
case 68=> watchers
case 69=> watchers_count


      }

  override def toString = s"Repo($archive_url, $archived, $assignees_url, $blobs_url, $branches_url, $clone_url, $collaborators_url, $comments_url, $commits_url, $compare_url, $contents_url, $contributors_url, $created_at, $default_branch, $deployments_url, $description, $downloads_url, $events_url, $fork, $forks, $forks_count, $forks_url, $full_name, $git_commits, $git_refs_url, $git_tags_url, $git_url, $has_downloads, $has_issues, $has_pages, $has_projects, $has_wiki, $homepage, $hooks_url, $html_url, $id, $issue_comment_url, $issue_events_url, $issues_url, $keys_url, $labels_url, $language, $languages_url, $license, $merges_url, $milestones_url, $mirror_url, $name, $notifications_url, $open_issues, $open_issues_count, $owner, $pulls_url, $pushed_at, $releases_url, $size, $ssh_url, $stargazers_count, $stargazers_url, $statuses_url, $subscribers_url, $subscription_url, $svn_url, $tags_url, $teams_url, $trees_url, $updated_at, $url, $watchers, $watchers_count)"
}
