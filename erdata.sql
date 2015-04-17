
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
		row serial,
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
select format('%I.%I',nspname,relname) as "table", array_agg_notnull(k.domain order by domain) as domains,
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

create or replace function er.namedef(_keys int8[], _id int8 default null)
returns int8 language plpgsql as $_$
declare
	id int8:=coalesce(_id,generate_id());
begin
	insert into er.data (e1,r,e2) select unnest, er.key('ключ именования'), id from unnest(_keys);
	return id;
end
$_$;

create or replace function er.namedef(_key_template text, _id int8 default null)
returns int8 language plpgsql as $_$
declare
	id int8:=coalesce(_id,generate_id());
begin
	insert into er.data (e1,r,t) select id, er.key('ключ именования'), _key_template;
	return id;
end
$_$;

-- определяющая онтология
select er.key_new('определяющее поле','metadata');
select er.key_new('определяемый тип','metadata');
select er.key_new('ключ определения','metadata');
select er.key_new('ключ именования','metadata');

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
select k.id as keyid, k.key, k.domain, n.e1 as namedef
from er.data n
join er.data ks on ks.r=er.key('ключ именования') and n.e1 in (ks.e1, ks.e2)
join er.keys k on k.id=ks.e1 or k.key like ks.t or k.key~ks.t
;
left join er.data ks on ks.r=er.key('ключ определения') and (ks.e2=t.e1)   (ks.e2=t.e1 and ks.r=er.key('ключ определения')) or (ks.e1=t.e1 and ks.r=er.key('шаблон ключа'))

create or replace function er.entities(_id int8, _name text default null, _type text default null, _domain text default null)
returns table(en int8, names text[], types text[], domains text[]) language plpgsql stable as $_$
declare
	_domains text[]:=coalesce(regexp_split_to_array(_domain,E',\\s*'),(select array_agg(distinct unnest) from (select unnest(s.domains) from er.storages s) s))||'{metadata}';
	en_filter text:=case when _id is not null then $$and case t.column when 'e1' then d.e1 when 'e2' then d.e2 end=$1$$ else '' end;
	name_filter text:=case when _name is not null then $$and string_agg(d.t,'')~$2$$ else '' end;
	type_filter text:=case when _type is not null then $$and $3=any(array_agg(type))$$ else '' end;
	dom_filter text:=case when _domain is not null then $$and $4=any(array_agg(t.domain))$$ else '' end;
begin
	return query execute $$
	with d as (
		$$||(select string_agg(format('select ''%s'' as "table","row",e1,r,e2,t from %s',s."table",s."table"),' union ') from er.storages s where array_intersect(_domains,s.domains)) ||$$
	)
	select
	case t.column when 'e1' then d.e1 when 'e2' then d.e2 end as en,
	case when 'персона'=any(array_agg_notnull(distinct type)) then array_agg_uniq(d.t order by d.row desc) else array_agg_uniq(d.t order by length(d.t)) end as names,
	array_agg_uniq(type order by typedef) as types, array_agg_uniq(t.domain order by t.domain) as domains
	from d
	left join er.typing t on t.keyid=d.r
	left join er.naming n on n.keyid=d.r
	where case t.column when 'e1' then d.e1 when 'e2' then d.e2 end is not null
	$$||en_filter||$$
	group by en
	having true
	$$||name_filter||$$
	$$||type_filter||$$
	$$||dom_filter||$$
	order by 2 nulls last, en
	$$ using _id,_name,_type,_domain;
end
$_$;

create or replace function er.record_of(_en int8, _domain text default null)
returns table("table" text, "row" int, e1 int8, name1 text, r int8, key text, domain text, e2 int8, name2 text, value text) language plpgsql as $_$
declare
	r record;
	_domains text[];
begin
	_domains=coalesce(regexp_split_to_array(_domain,E',\\s*'),(select array_agg(distinct unnest) from (select unnest(s.domains) from er.storages s) s))||'{metadata}';
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

