create or replace function trans_func(amount number)
returns string
language sql 
as
$$
select case when amount > 500 then 'High'
        when amount between 100 and 200 then 'Medium'
        else 'Low' end
 
$$
;   
 
grant USAGE on FUNCTION trans_func(NUMBER) to role PC_DBT_ROLE;
 
 
select *, trans_func(amount) as risk_level from transacation_raw;
 
