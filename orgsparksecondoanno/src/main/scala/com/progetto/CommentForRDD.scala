package com.progetto

case class CommentForRDD(
                  _links: _Links2,
                  author_association:String,
                  body:String,
                  commit_id:String,
                  created_at:String,
                  diff_hunk:String,
                  html_url:String,
                  id:BigInt,
                  in_reply_to_id:BigInt,
                  issue_url:String,
                  line:BigInt,
                  original_commit_id:String,
                  original_position:BigInt,
                  path:String,
                  position:BigInt,
                  pull_request_review_id:BigInt,
                  pull_request_url:String,
                  updated_at:String,
                  url:String,
                  user: UserForRDD
                  )