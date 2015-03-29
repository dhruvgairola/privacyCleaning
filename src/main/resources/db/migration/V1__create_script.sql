drop table if exists employee;
drop table if exists dependencies;
drop table if exists edges;
drop table if exists nodes;
drop table if exists constraints;
drop table if exists datasets;
drop table if exists master_datasets;

-- This doesn't work in DBVis, use pgadmin
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = now(); 
   RETURN NEW;
END;
$$ language 'plpgsql';

--nodes are more general than dependencies
create table nodes (
	id serial primary key,
	is_compound boolean,
	name varchar(10) not null,
	description varchar(255),
    updated_at timestamp
);

create table edges (
	id serial primary key,
	is_dotted boolean,
	first_node_id integer not null,
	second_node_id integer not null,
	weight double precision,
    updated_at timestamp,
	constraint fk_head foreign key(first_node_id) references nodes(id) on update cascade on delete cascade,
	constraint fk_tail foreign key(second_node_id) references nodes(id) on update cascade on delete cascade,
	constraint ct_no_self_loops check (first_node_id <> second_node_id)
);

create index idx_head on edges(first_node_id);
create index idx_tail on edges(second_node_id);
-- each pair is unique
create unique index idx_pair on edges(least(first_node_id, second_node_id), greatest(first_node_id, second_node_id));

create table dependencies (
	id integer primary key,
	dependency_type varchar(255),
	determinant_node_id integer not null,
	dependent_node_id integer not null,
    updated_at timestamp,
	constraint fk_determinant foreign key(determinant_node_id) references nodes(id),
	constraint fk_dependent foreign key(dependent_node_id) references nodes(id)
);

create table employee (
	id integer primary key,
	employee_id integer,
	appraisal varchar(255),
	business_experience integer,
	compensation integer,
	designation varchar(255),
	employee_bonus integer
);

create table datasets (
	id serial primary key,
	uid bigint,
	name varchar(255),
	url varchar(255),
	updated_at timestamp,
	numRecords bigint,
	constraint fk_dataset_id foreign key(uid) references users(id)
);

create table master_datasets (
	id serial primary key,
	uid bigint,
	tid bigint,
	url varchar(255),
	updated_at timestamp,
	numRecords bigint,
	constraint fk_master_dataset_id foreign key(uid) references users(id),
	constraint fk_target_id foreign key(tid) references datasets(id)
);

create table constraints (
	id serial primary key,
	datasetid bigint,
	antecedent varchar(255),
	consequent varchar(255),
	updated_at timestamp,
	unique (datasetid, antecedent, consequent),
	constraint fk_constraint_dataset_id foreign key(datasetid) references datasets(id)
);

create index idx_constraint on constraints(datasetid, antecedent, consequent);

CREATE TRIGGER update_node BEFORE INSERT OR UPDATE ON nodes FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER update_edges BEFORE INSERT OR UPDATE ON edges FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER update_dependencies BEFORE INSERT OR UPDATE ON dependencies FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER update_datasets BEFORE INSERT OR UPDATE ON datasets FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER update_constraints BEFORE INSERT OR UPDATE ON constraints FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
CREATE TRIGGER update_master_datasets BEFORE INSERT OR UPDATE ON master_datasets FOR EACH ROW EXECUTE PROCEDURE update_timestamp();