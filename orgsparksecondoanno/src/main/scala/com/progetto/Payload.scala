package com.progetto

case class Payload(
                  action:String,
                  before:String,
                  comment: Comment,
                  commits: CommitPrincipale,
                  description:String,
                  distinct_size:BigInt,
                  forkee: Forkee,
                  head:String,
                  issue: IssuePrincipale,
                  master_branch:String,
                  member: Member,
                  number:BigInt,
                  pages: Pages,
                  pull_request: PullRequestPrincipale,
                  push_id:BigInt,
                  pusher_type:String,
                  ref:String,
                  ref_type:String,
                  release: Release,
                  size:BigInt
                  )