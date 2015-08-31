
create schema er;

create or replace function er.util_create_keys()
returns text language plpgsql as $_$
begin
	create table er.keys (
		id int8 default generate_id() primary key,
		domain text,
		key text,
		unique(domain,key)
	);
	create index on er.keys(domain);
	create index on er.keys(key);
	return 'ok';
end
$_$;

select * from er.util_create_keys();

create or replace function er.util_create_storage(_table text, _value_columns text[] default '{e2,t}')
returns table(status text) language plpgsql volatile as $_$
declare
	r record;
	c text;
begin

	select string_agg(format ('%I %s',u,type),', ') as columns from (
	select u,case u when 'e2' then 'int8' when 't' then 'text' when 'j' then 'json' when 'n' then 'numeric' when 'i' then 'int' end as type from unnest(_value_columns) as u
	) u into r;

	execute $$create table $$||_table||$$ (
		row serial primary key,
		e1 int8,
		r int8 references er.keys(id)
		$$||format('%s',','||r.columns)||$$
	)$$;
	return query select format('table %s created',_table);
	foreach c in array '{e1,r}'||_value_columns loop
		execute format('create index on %s(%s)',_table,c);
		return query select format('index on %s created',c);
	end loop;
end
$_$;
select * from er.util_create_storage('er.data');

create or replace function er.key(_key text, _domain text default null)
returns int8 language sql stable
as $$
	select (select id from er.keys where key like _key and domain like coalesce(_domain,'%'))
$$;

create or replace function er.key(_id int8)
returns text language sql stable
as $$
	select key from er.keys where id=_id
$$;

create or replace function er.keys_like(_key text, _domain text default null)
returns setof int8 language sql stable
as $$
	select id from er.keys where key like _key and domain like coalesce(_domain,'%')
$$;

create or replace function er.keys(_key text, _domain text default null)
returns int8[] language sql stable
as $$
	select array_agg(id) from er.keys where key like _key and domain like coalesce(_domain,'%')
$$;

create or replace function er.keys(_keys text[], _domain text default null)
returns int8[] language sql stable
as $$
	select array_agg(k.id) from unnest(_keys) as u join er.keys k on k.key like u and domain like coalesce(_domain,'%');
$$;

create or replace function er.key_new(_key text, _domain text)
returns int8 language sql
as $_$
	insert into er.keys(key,domain)
	select _key,_domain
	where not exists (select 1 from er.keys where key=_key and domain=_domain);
	select id from er.keys where key=_key and domain=_domain
$_$;

select er.key_new('хранилище домена','metadata');
insert into er.data(r,t) values (er.key('хранилище домена','metadata'),'er.data');

create view er.storages as
select format('%s%I',nullif(nspname,'public')||'.',relname) as "table", array_agg_uniq(k.domain order by domain) as domains,
(select array_agg(attname order by attnum) from pg_attribute where attrelid=r.oid and attnum>0) as columns,
(select array_agg(typname order by attnum) from pg_attribute a join pg_type t on t.oid=a.atttypid where attrelid=r.oid and attnum>0) as types
from pg_class r
join pg_namespace n on n.oid=r.relnamespace
left join (
er.keys k join er.data d on d.r=k.id 
) on k.key='хранилище домена' and (d.t=format('%I.%I',nspname,relname) or d.t=format('%I',coalesce(nullif(nspname,'public')||'.','')||relname))
where r.relkind='r' and nspname not in ('pg_catalog','information_schema')
and (select array_agg(attname order by attnum) from pg_attribute where attrelid=r.oid and attnum>0 and attnum<4)='{row,e1,r}'::name[]
group by r.oid, nspname, relname
;

-- определения типов и именования
create or replace function er.typedef(_col text, _keys int8[], _type text, _id int8 default null)
returns int8 language plpgsql as $_$
declare
	id int8:=coalesce(_id,generate_id());
begin
	insert into er.data (e1,r,t) select id, er.key('определяющее поле'), _col;
	insert into er.data (e1,r,t) select id, er.key('определяемый тип'), _type where _type is not null;
	insert into er.data (e1,r,e2) select unnest, er.key('ключ определения'), id from unnest(_keys);
	return id;
end
$_$;

create or replace function er.typedef(_col text, _key_template text, _type text, _id int8 default null)
returns int8 language plpgsql as $_$
declare
	id int8:=coalesce(_id,generate_id());
begin
	insert into er.data (e1,r,t) select id, er.key('определяющее поле'), _col;
	insert into er.data (e1,r,t) select id, er.key('определяемый тип'), _type where _type is not null;
	insert into er.data (e1,r,t) select id, er.key('ключ определения'), _key_template;
	return id;
end
$_$;

create or replace function er.namedef(_keys int8[], _ordering text, _id int8 default null)
returns int8 language plpgsql as $_$
declare
	id int8:=coalesce(_id,generate_id());
begin
	insert into er.data (e1,r,e2) select unnest, er.key('ключ именования'), id from unnest(_keys);
	insert into er.data (e1,r,t) select id, er.key('порядок именования'), _ordering where _ordering is not null;
	return id;
end
$_$;

create or replace function er.namedef(_key_template text, _ordering text, _id int8 default null)
returns int8 language plpgsql as $_$
declare
	id int8:=coalesce(_id,generate_id());
begin
	insert into er.data (e1,r,t) select id, er.key('ключ именования'), _key_template;
	insert into er.data (e1,r,t) select id, er.key('порядок именования'), _ordering where _ordering is not null;
	return id;
end
$_$;

-- определяющая онтология
select er.key_new('определяющее поле','metadata');
select er.key_new('определяемый тип','metadata');
select er.key_new('ключ определения','metadata');
select er.key_new('ключ именования','metadata');
select er.key_new('порядок именования','metadata');

select * from er.typedef('e1',er.keys('определяемый тип','metadata'),'определение');
select * from er.typedef('e1',er.keys('ключ именования','metadata'),'именование');

create view er.typing as
select k.id as keyid, k.key, k.domain, c.t as column, t.t as type, t.e1 as typedef
from er.data t
join er.data c on c.e1=t.e1 and c.r=er.key('определяющее поле','metadata')
left join er.data ks on ks.r=er.key('ключ определения') and t.e1 in (ks.e1, ks.e2)
left join er.keys k on k.id=ks.e1 or k.key like ks.t or k.key~ks.t
where t.r=er.key('определяемый тип')
;

create view er.naming as
select k.id as keyid, k.key, k.domain, n.e1 as namedef,o.t as ordering
from er.data n
join er.data ks on ks.r=er.key('ключ именования') and n.e1 in (ks.e1, ks.e2)
join er.keys k on k.id=ks.e1 or k.key like ks.t or k.key~ks.t
left join er.data o on o.e1=n.e1 and o.r=er.key('порядок именования')
;

create type er.entity as (en int8, names text[], types text[], domains text[]);

create or replace function er.entities(_id int8, _name text default null, _type text default null, _domain text default null, _limit int default null)
returns setof er.entity language plpgsql stable as $_$
declare
	_domains text[]:=coalesce(regexp_split_to_array(_domain,E',\\s*'),(select array_agg(distinct unnest) from (select unnest(s.domains) from er.storages s) s))||'{metadata}';
	en_filter text:=case when _id is not null then $$and case when t.column='e1' then d.e1 when t.column='e2' then d.e2 when n.namedef is not null then d.e1 end=$1$$ else '' end;
	name_filter text:=case when _name is not null then $$and bool_or(d.t~*simple_regexp($2))$$ else '' end;
	type_filter text:=case when _type is not null then $$and $3=any(array_agg(type))$$ else '' end;
	dom_filter text:=case when _domain is not null then $$and $4=any(array_agg(t.domain))$$ else '' end;
begin
	return query execute $$
	with d as (
		$$||(select string_agg(format('select ''%s'' as "table","row",e1,r,e2,t from %s',s."table",s."table"),' union ') from er.storages s where array_intersect(_domains,s.domains)) ||$$
	)
	select
	case when t.column='e1' then d.e1 when t.column='e2' then d.e2 when n.namedef is not null then d.e1  end as en,
	case when 'earliest'=any(array_agg(n.ordering)) then array_agg_uniq(d.t order by d.row) when 'latest'=any(array_agg(n.ordering)) then array_agg_uniq(d.t order by d.row desc) when 'shortest'=any(array_agg(n.ordering)) then array_agg_uniq(d.t order by length(d.t)) when 'longest'=any(array_agg(n.ordering)) then array_agg_uniq(d.t order by length(d.t) desc) else array_agg(d.t) end as names,
	array_agg_uniq(type order by typedef) as types, array_agg_uniq(t.domain order by t.domain) as domains
	from d
	left join er.typing t on t.keyid=d.r
	left join er.naming n on n.keyid=d.r
	where case when t.column='e1' then d.e1 when t.column='e2' then d.e2 when n.namedef is not null then d.e1 end is not null
	$$||en_filter||$$
	group by en
	having true
	$$||name_filter||$$
	$$||type_filter||$$
	$$||dom_filter||$$
	order by 2 nulls last, en
	$$||format('%s','limit '||_limit)||$$
	$$ using _id,_name,_type,_domain;
end
$_$;

create or replace function er.record_of(_en int8, _domain text default null)
returns table("table" text, "row" int, e1 int8, name1 text, r int8, key text, domain text, e2 int8, name2 text, value text) language plpgsql as $_$
declare
	_domains text[]:=coalesce(regexp_split_to_array(_domain,E',\\s*'),(select array_agg(distinct unnest) from (select unnest(s.domains) from er.storages s) s))||'{metadata}';
begin
	return query execute $$
	with d as (
		$$||(select string_agg(format('select ''%s'' as "table","row",e1,r,e2,t from %s',s."table",s."table"),' union ') from er.storages s where array_intersect(_domains,s.domains)) ||$$
	)
	select d."table", d.row, d.e1, n1.t as name1,d.r,k.key, k.domain, d.e2, n2.t as name2, d.t
	from d
	left join d n1 on n1.e1=d.e1 and n1.r in (select keyid from er.naming) and not exists (select 1 from d where e1=n1.e1 and r=n1.r and length(t)<length(n1.t) and row<>n1.row)
	left join d n2 on n2.e1=d.e2 and n2.r in (select keyid from er.naming) and not exists (select 1 from d where e1=n2.e1 and r=n2.r and length(t)<length(n2.t) and row<>n2.row)
	left join er.keys k on k.id=d.r
	where $1 in (d.e1,d.e2)
	$$ using _en;
end
$_$;

create type er.row as ("column" text, type text, value text);

create or replace function er.row(_table text, _row int)
returns setof er.row language plpgsql as $_$
declare
	t record;
	q text;
begin
	select * from er.storages where "table"=_table into strict t;
	return query execute (
	select format($$select (u).* from (select unnest(row('table',null,'%s')::er.row||array[%s]::er.row[]) u from %s where "row"=%s::int) u$$,
		_table,
		string_agg(format($$row('%s','%s', %I)$$,t.columns[i], t.types[i], t.columns[i]),', '),
		regexp_replace(_table,';','_','g'),quote_nullable(_row)
	) from generate_subscripts(t.columns,1) as i
	);
end
$_$;

create or replace function er.chrow (_table text, _row int, _columns text[], _values text[])
returns table("table" text, "row" int, action text) language plpgsql as $_$
declare
	a record;
	r record;
begin
	with x as (
	select * from unnest(_columns) with ordinality as col(c,o)
	join unnest(_values) with ordinality as val(v,o) on val.o=col.o
	),
	tables as (
	select *, coalesce(regexp_replace(dest,';','_'),'') as dest_name, coalesce(regexp_replace(src,';','_'),'') as src_name from (select (select v from x where c='table') as dest, _table as src) s
	),
	types as (
	select s."table", (u).* from tables, er.storages s,unnest(columns,types) with ordinality as u("column",type,o) where s."table" in (src,dest)
	),
	m as (
	select format('insert into %s (%s) ',(select dest_name from tables),string_agg(format('%I',"column"),', '))||
	format ('select %s from %s where "row"=$1',string_agg(format('%s::%s',case when exists (select 1 from x where c="column") then (select quote_nullable(v) from x where c="column") else quote_ident("column") end,type),', '),(select src_name from tables))||
	' returning *' as _copy,
	format ('delete from %s where "row"=$1 returning *',(select src_name from tables)) as _delete
	from types t,tables where t."table"=src and exists (select 1 from types t2 where t2."table"=dest and t2."column"=t."column")
	and case when exists (select 1 from x where c='row') then true else "column"<>'row' end
	),
	u as (
	select format ('update %s set %s where "row"=$1 returning *',(select dest_name from tables),string_agg(format('%I=%L::%s',x.c,x.v,type),', ')) as _update,
	format ('insert into %s (%s) values ( %s ) returning *',(select dest_name from tables),string_agg(format('%I',x.c),', '),string_agg(format('%L::%s',x.v,type),', ')) as _insert
	from tables,x,types where x.c=types."column" and types."table"=tables.dest
	)
	select * from tables,m,u
	into a;
	if a.src<>a.dest and _row is not null then
		execute a._copy using _row into r;
		if r.row is null then return query select a.src,_row,'does not exist'::text; return; end if;
		return query select a.dest,r.row,'inserted with update'::text;
	end if;
	if a.src<>a.dest or a.dest is null then
		execute a._delete using _row into r;
		if r.row is null then return query select a.src,_row,'does not exist'::text; return; end if;
		return query select a.src,_row,'deleted'::text;
	end if;
	if a.src is null or _row is null then
		execute a._insert using _row into r;
		return query select a.dest,r.row,'inserted'::text;
	end if;
	if a.src=a.dest and _row is not null then
		execute a._update using _row into r;
		return query select a.dest,_row,case when r.row is null then 'does not exist' else 'updated' end::text;
	end if;
end
$_$;

create or replace function er.tree_from(_root int8, _relations int8[], _reverse boolean default false, _depth int default null, _floor int default 0, _domain text default null)
returns table(path int8[], r int8, leaf boolean) as
$_$
declare
	_domains text[]:=coalesce(regexp_split_to_array(_domain,E',\\s*'),(select array_agg(distinct unnest) from (select unnest(s.domains) from er.storages s) s))||'{metadata}';
	depth_check varchar:='';
	floor_check varchar:='';
	abs int8[];
begin
	select array_agg(abs(unnest)) from unnest(_relations) into abs;
	if _reverse then
		select array_agg(-unnest) from unnest(_relations) into _relations;
	end if;
	if _depth is not null then depth_check:=' and array_length(r.p,1)-2<'||_depth; end if;
	if _floor<>0 then floor_check:=' and array_length(r.p,1)-1>='||_floor; end if;
	return query execute $$
		with recursive r(p,r,c) as (
			select array[$1],null::int8,false
			union
			select p||array[case when -d.r=any($2) then d.e2 else d.e1 end],d.r,case when -d.r=any($2) then d.e2 else d.e1 end=any(p)
			from ($$||(select string_agg(format('select ''%s'' as "table","row",e1,r,e2,t from %s',s."table",s."table"),' union ') from er.storages s where array_intersect(_domains,s.domains))||$$) d
			join r on r.p[array_length(r.p,1)]=case when -d.r=any($2) then d.e1 else d.e2 end and d.r = any($3)
			where not c $$||depth_check||$$
		) select r.p,r.r,not exists (select 1 from r z where p[1:array_length(p,1)-1]=r.p) from r
		where true $$||floor_check||depth_check
	using _root,_relations,abs;
end
$_$ language plpgsql stable;

