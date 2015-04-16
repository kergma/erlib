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

create or replace function array_intersection(a anyarray, b anyarray)
returns anyarray language sql immutable
as $_$
	select array (select unnest(a) intersect select unnest(b));
$_$;

create or replace function array_intersect(a anyarray, b anyarray)
returns boolean language sql immutable
as $_$
	select exists (select 1 from unnest(a) as x(a) where a=any(b));
$_$;

create or replace function create_indexes(_table regclass, _want text default '%')
returns table(attname name, index_name regclass, status text) language plpgsql volatile as $_$
declare
	r record;
begin
	_want=(select '(a.attname like '''||string_agg(w,''' or a.attname like ''')||''')' from unnest(regexp_split_to_array(_want,E'\\s*,\\s*')) as w);

	for r in
		execute 'select a.attname,a.attnum,i.*
		from pg_class c
		join pg_attribute a on a.attrelid=c.oid
		left join pg_index i on i.indrelid=c.oid and i.indkey=array[a.attnum]::int2vector
		where c.oid=$1 and a.attnum>0 and '||_want||'
		order by a.attnum
		' using _table
	loop
		if r.indexrelid is null then
			execute format('create index on %s (%s)',_table,r.attname);
		end if;
		return query select r.attname,(select indexrelid::regclass from pg_index where indrelid=_table and indkey=array[r.attnum]::int2vector),case when r.indexrelid is not null then 'already exists' else 'created' end;
	end loop;
end;
$_$;

commit;
