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

commit;
