with stg__user as (

	select "USER_ID"::varchar			as user_id
		 , "EMPLOYEE_ID"::varchar		as employee_id
		 , "STARTED_ONBOARDING_AT"		as started_onboarding_at
		 , "KYC_STATUS"					as kyc_status
		 , "DATE_OF_BIRTH"				as date_of_birth
	from assessment.public."EA_assignment_users"

)

, stg__loan as (

	select "LOAN_UUID"::uuid	as loan_uuid
		 , "USER_ID"::varchar	as user_id
		 , "AMOUNT"				as amount
		 , "APPLIED_AT"			as applied_at
		 , "SETTLED_AT"			as settled_at
		 , "FULLY_REPAID_AT"	as fully_repaid_at
		 , "IS_CONFIRMED"		as is_confirmed
		 , "IS_INSTANT"			as is_instant
	from assessment.public."EA_assignment_loans"

)

, stg__webhook as (

	select "USER_ID"::varchar			as user_id
		 , "SUBJECT_TYPE"				as subject_type
		 , "PAYLOAD"					as payload
		 , "RESPONSE_STATUS"::varchar	as response_status
	from assessment.public."EA_assignment_webhooks"

)

, dim__user as ( select * from stg__user )

, fct__loan as ( select * from stg__loan )

, fct__user_event as (

	select user_id
		 , subject_type
		 , response_status
		 , payload->'event'->>'id'						as event_id
	 	 , payload->'event'->'data'->>'employee_id'		as employee_id
	 	 , payload->'event'->'data'->>'ip_address'   	as ip_address
		 , payload->'event'->>'type'					as event_type
		 , to_timestamp(payload->'event'->>'timestamp'
		 			  , 'YYYY-MM-DD"T"HH24:MI:SS"TZH')	as event_timestamp
	from stg__webhook
	where subject_type = 'App\Models\User'

)

, agg__loan__user_id as (

	select user_id
		 , min(case when is_confirmed = true 
		 			then applied_at
					else null
			   end)								as first_loan_confirmed_at
	     , min(fully_repaid_at)					as first_loan_fully_repaid_at
		 , sum(case when is_confirmed = true 
		 			then 1
					else 0
			   end)								as lifetime_loans_confirmed_count
		 , sum(case when fully_repaid_at is not null
		 			then amount
					else 0
			   end)								as lifetime_full_repaid_loan_amount
	from fct__loan
	group by 1

)

, agg__user_event__user_id as (

	select user_id
		 , min(case when event_type = 'enrollment.pass'
		 			then event_timestamp
					else null
			   end) 							as enrollment_completed_at
	from fct__user_event
	group by 1

)

, rpt__user_lending_readiness as (

	select dim__user.user_id
		 , dim__user.started_onboarding_at
		 , agg__user_event__user_id.enrollment_completed_at
		 , case when dim__user.kyc_status = 'Pass'
		 			and agg__user_event__user_id.enrollment_completed_at is not null
				then true
				else false
		   end 																	as has_completed_enrollment
		 , agg__loan__user_id.first_loan_confirmed_at
		 , agg__loan__user_id.first_loan_fully_repaid_at
		 , coalesce(agg__loan__user_id.lifetime_loans_confirmed_count, 0)		as lifetime_loans_confirmed_count	
		 , coalesce(agg__loan__user_id.lifetime_full_repaid_loan_amount, 0)		as lifetime_full_repaid_loan_amount
		 , extract(day from agg__user_event__user_id.enrollment_completed_at 
		                  - dim__user.started_onboarding_at) 					as days_to_create_account
		 , extract(day from agg__loan__user_id.first_loan_confirmed_at 
		                  - agg__user_event__user_id.enrollment_completed_at) 	as days_to_first_loan
		 , case when first_loan_fully_repaid_at is not null
		 		then true
				else false
		   end																	as is_returning
	from dim__user
	left join agg__user_event__user_id
		on dim__user.user_id = agg__user_event__user_id.user_id
	left join agg__loan__user_id
		on dim__user.user_id = agg__loan__user_id.user_id

)

select *
from rpt__user_lending_readiness