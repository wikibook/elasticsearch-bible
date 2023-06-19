package kr.co.wikibook.action

import org.elasticsearch.action.ActionType

const val NAME = "indices:data/read/dump"
object PartialDumpAction : ActionType<PartialDumpResponse>(NAME, ::PartialDumpResponse)