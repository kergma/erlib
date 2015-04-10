
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

create or replace function er.util_create_storage(_table text)
returns text language plpgsql volatile as $_$
begin
	execute $$create table $$||_table||$$ (
		row serial,
		e1 int8,
		r int8 references er.keys(id),
		e2 int8,
		value text
	)$$;
	execute 'create index on '||_table||'(e1)';
	execute 'create index on '||_table||'(r)';
	execute 'create index on '||_table||'(e2)';
	execute 'create index on '||_table||'(value)';

	return 'ok';
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

create or replace function er.key_new(_key text, _domain text)
returns int8 language sql
as $_$
	insert into er.keys(key,domain)
	select _key,_domain
	where not exists (select 1 from er.keys where key=_key and domain=_domain);
	select id from er.keys where key=_key and domain=_domain
$_$;

select er.key_new('хранилище домена','metadata');
insert into er.data(r,value) values (er.key('хранилище домена','metadata'),'er.data');

create view er.storages as
select format('%I.%I',nspname,relname) as "table", array_agg_notnull(k.domain order by domain) as domains
from pg_class r
join pg_namespace n on n.oid=r.relnamespace
left join (
er.keys k join er.data d on d.r=k.id 
) on k.key='хранилище домена' and (d.value=format('%I.%I',nspname,relname) or d.value=format('%I',coalesce(nullif(nspname,'public')||'.','')||relname))
where r.relkind='r' and nspname not in ('pg_catalog','information_schema')
and (select array_agg(attname order by attnum) from pg_attribute where attrelid=r.oid and attnum>0)='{row,e1,r,e2,value}'::name[]
group by nspname, relname
;

