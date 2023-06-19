package kr.co.wikibook.action

import org.elasticsearch.action.ActionType

const val NAME = "cluster:admin/start_check_condition"
object StartCheckConditionAction : ActionType<StartCheckConditionResponse>(NAME, ::StartCheckConditionResponse)