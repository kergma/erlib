-- аггрегаты

create or replace function join_aggregate(text, text, text)
returns text as
$body$
begin  
	if length($1)>0 and length($3)>0 then
		return $1 || $2 || $3;
	elsif length($2)>0 then
		return $3;
	end if;
	return $1;
end;
$body$
language plpgsql volatile
;
drop aggregate if exists join(text,text) cascade;
create aggregate join(text,text) ( sfunc=join_aggregate,stype=text, initcond='' );

create or replace function comma_aggregate(text, text)
returns text as
$body$
begin  
	if length($1)>0 and length($2)>0 then
		return $1 || ', ' || $2;
	elsif length($2)>0 then
		return $2;
	end if;
	return $1;
end;
$body$
language plpgsql volatile
;
drop aggregate if exists comma(text) cascade;
create aggregate comma(text) ( sfunc=comma_aggregate,stype=text, initcond='' );

create or replace function shortest_aggregate(text, text)
returns text as
$body$
begin  
	if length($1)<length($2) then
		return $1;
	end if;
	return $2;
end;
$body$
language plpgsql volatile
;
drop aggregate if exists shortest(text) cascade;
create aggregate shortest(text) ( sfunc=shortest_aggregate,stype=text );

create or replace function longest_aggregate(text, text)
returns text as
$body$
begin  
	if length($1)>length($2) then
		return $1;
	end if;
	return $2;
end;
$body$
language plpgsql volatile
;
drop aggregate if exists longest(text) cascade;
create aggregate longest(text) ( sfunc=longest_aggregate,stype=text );

create or replace function array_agg_notnull_aggregator(a anyarray, b anyelement)
returns anyarray language plpgsql immutable strict as $_$
begin
	if b is not null then
		a=array_append(a,b);
	end if;
	return a;
end;
$_$;

drop aggregate if exists array_agg_notnull(anyelement) cascade;
create aggregate array_agg_notnull(anyelement) ( sfunc=array_agg_notnull_aggregator,stype=anyarray, initcond='{}');

commit;
