
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

